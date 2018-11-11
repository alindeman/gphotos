package gphotos

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	photoslibrary "google.golang.org/api/photoslibrary/v1"
)

const (
	MaxUploadTokensPerCreateMediaItemsCall = 50
)

type UploadToken string

type Uploader struct {
	client *http.Client

	service *photoslibrary.Service
}

func NewUploader(client *http.Client) *Uploader {
	// Only reason for error is if client is nil
	service, _ := photoslibrary.New(client)

	return &Uploader{
		client:  client,
		service: service,
	}
}

func (u *Uploader) Upload(filename string, r io.Reader) (UploadToken, error) {
	req, err := http.NewRequest("POST", "https://photoslibrary.googleapis.com/v1/uploads", r)
	if err != nil {
		return "", err
	}

	req.Header.Set("content-type", "application/octet-stream")
	req.Header.Set("x-goog-upload-file-name", filename)
	req.Header.Set("x-goog-upload-protocol", "raw")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	rawToken, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return UploadToken(rawToken), nil
}

func (u *Uploader) CreateMediaItems(tokens []UploadToken) error {
	if len(tokens) > MaxUploadTokensPerCreateMediaItemsCall {
		return fmt.Errorf("too many tokens, got %v, cannot handle more than %v", len(tokens), MaxUploadTokensPerCreateMediaItemsCall)
	}

	newMediaItems := make([]*photoslibrary.NewMediaItem, 0, len(tokens))
	for _, token := range tokens {
		newMediaItems = append(newMediaItems, &photoslibrary.NewMediaItem{
			SimpleMediaItem: &photoslibrary.SimpleMediaItem{
				UploadToken: string(token),
			},
		})
	}

	req := &photoslibrary.BatchCreateMediaItemsRequest{
		NewMediaItems: newMediaItems,
	}

	_, err := u.service.MediaItems.BatchCreate(req).Do()
	if err != nil {
		return err
	}

	return nil
}
