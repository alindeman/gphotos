package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/alindeman/gphotos"
	retry "github.com/avast/retry-go"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	photoslibrary "google.golang.org/api/photoslibrary/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)
		os.Exit(255)
	}
}

func run() error {
	ctx := context.Background()

	var credentialsFile, tokenFile string
	var uploadConcurrency int
	flag.StringVar(&credentialsFile, "credentials-file", "", "OAuth Client ID configuration file (downloadable from https://console.cloud.google.com/apis/credentials)")
	flag.StringVar(&tokenFile, "token-file", "", "File to load or store an OAuth token")
	flag.IntVar(&uploadConcurrency, "upload-concurrency", 10, "Number of threads that are uploading files concurrency")
	flag.Parse()

	if credentialsFile == "" {
		return errors.New("missing required flag: credentials-file")
	} else if tokenFile == "" {
		return errors.New("missing required flag: token-file")
	} else if uploadConcurrency <= 0 {
		return errors.New("upload-concurrency must be greater than 0")
	}

	credentialJSON, err := ioutil.ReadFile(credentialsFile)
	if err != nil {
		return errors.Wrap(err, "failed to read credentials")
	}

	oauthConfig, err := google.ConfigFromJSON(credentialJSON, photoslibrary.PhotoslibraryScope)
	if err != nil {
		return errors.Wrap(err, "failed to parse credentials")
	}

	token, err := readTokenFromFile(tokenFile)
	if os.IsNotExist(err) {
		token, err = fetchToken(ctx, oauthConfig)
		if err != nil {
			return errors.Wrap(err, "failed to fetch token")
		}
	} else if err != nil {
		return errors.Wrap(err, "failed to load token from file")
	}

	if err := saveTokenToFile(tokenFile, token); err != nil {
		return errors.Wrap(err, "failed to save token to file")
	}

	oauthClient := oauthConfig.Client(ctx, token)
	u := gphotos.NewUploader(oauthClient)

	g, ctx := errgroup.WithContext(ctx)
	messages := make(chan string)
	defer close(messages)

	// 1 thread to shove filenames into filenames channel
	filenames := make(chan string)
	g.Go(func() error {
		defer close(filenames)

		for _, filename := range flag.Args() {
			select {
			case filenames <- filename:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	// N threads uploading files
	uploadTokens := make(chan gphotos.UploadToken, gphotos.MaxUploadTokensPerCreateMediaItemsCall)
	var wg sync.WaitGroup
	for i := 0; i < uploadConcurrency; i++ {
		wg.Add(1)

		g.Go(func() error {
			defer wg.Done()

			for filename := range filenames {
				var uploadToken gphotos.UploadToken
				err := retry.Do(func() error {
					var err error
					uploadToken, err = uploadFile(u, filename)
					return err
				})
				if err != nil {
					return errors.Wrapf(err, "error uploading %q", filename)
				}

				select {
				case uploadTokens <- uploadToken:
				case <-ctx.Done():
					return ctx.Err()
				}

				select {
				case messages <- fmt.Sprintf("uploaded %q", filename):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})
	}
	go func() {
		wg.Wait()
		close(uploadTokens)
	}()

	// 1 thread creating media items in batches
	g.Go(func() error {
		uploadBatch := func(batch []gphotos.UploadToken) error {
			if len(batch) == 0 {
				return nil
			}

			err := retry.Do(func() error {
				return u.CreateMediaItems(batch)
			})
			if err != nil {
				return err
			}

			select {
			case messages <- fmt.Sprintf("uploaded batch of %v photos", len(batch)):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		currentBatch := []gphotos.UploadToken{}
		for uploadToken := range uploadTokens {
			currentBatch = append(currentBatch, uploadToken)
			if len(currentBatch) >= gphotos.MaxUploadTokensPerCreateMediaItemsCall {
				if err := uploadBatch(currentBatch); err != nil {
					return err
				}
				currentBatch = []gphotos.UploadToken{}
			}
		}

		// Upload final batch, if any
		return uploadBatch(currentBatch)
	})

	go func() {
		for message := range messages {
			fmt.Printf("%v\n", message)
		}
	}()

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to upload photos")
	}
	return nil
}

func readTokenFromFile(tokenFile string) (*oauth2.Token, error) {
	token := new(oauth2.Token)

	f, err := os.Open(tokenFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(token); err != nil {
		return nil, err
	}

	return token, err
}

func fetchToken(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	url := config.AuthCodeURL("state", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following URL, then paste the authorization token: %v\n\n", url)
	fmt.Printf("Auth code: ")

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		return nil, err
	}

	return config.Exchange(ctx, authCode)
}

func saveTokenToFile(tokenFile string, token *oauth2.Token) error {
	f, err := os.OpenFile(tokenFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(token)
}

func uploadFile(u *gphotos.Uploader, filename string) (gphotos.UploadToken, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	return u.Upload(filepath.Base(filename), f)
}
