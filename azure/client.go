package azure

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

// blobProps holds the subset of blob properties that s2 cares about.
type blobProps struct {
	contentLength int64
	lastModified  time.Time
	metadata      map[string]*string
}

type listBlobsResult struct {
	items      []blobItem
	prefixes   []string
	nextMarker string
}

type blobItem struct {
	name          string
	contentLength int64
	lastModified  time.Time
	metadata      map[string]*string
}

// azureClient abstracts the Azure Blob SDK so that tests can swap in a mock.
type azureClient interface {
	getProperties(ctx context.Context, container, blob string) (blobProps, error)
	downloadStream(ctx context.Context, container, blobName string, offset, count int64) (io.ReadCloser, error)
	upload(ctx context.Context, container, blobName string, body io.Reader, metadata map[string]*string) error
	deleteBlob(ctx context.Context, container, blobName string) error
	setMetadata(ctx context.Context, container, blobName string, metadata map[string]*string) error
	copyBlob(ctx context.Context, container, src, dst string) error
	listBlobs(ctx context.Context, container, prefix, delimiter string, maxResults int32, marker string) (listBlobsResult, error)
	signedURL(container, blobName string, method string, expiry time.Time) (string, error)
	serviceURL() string
}

// --- SDK implementation ---

type sdkClient struct {
	client    *azblob.Client
	sharedKey *azblob.SharedKeyCredential
}

func (c *sdkClient) serviceURL() string {
	return c.client.URL()
}

func (c *sdkClient) containerClient(name string) *container.Client {
	return c.client.ServiceClient().NewContainerClient(name)
}

func (c *sdkClient) blobClient(ctr, blobName string) *blob.Client {
	return c.containerClient(ctr).NewBlobClient(blobName)
}

func (c *sdkClient) getProperties(ctx context.Context, ctr, blobName string) (blobProps, error) {
	resp, err := c.blobClient(ctr, blobName).GetProperties(ctx, nil)
	if err != nil {
		return blobProps{}, err
	}
	return blobProps{
		contentLength: derefInt64(resp.ContentLength),
		lastModified:  derefTime(resp.LastModified),
		metadata:      resp.Metadata,
	}, nil
}

func (c *sdkClient) downloadStream(ctx context.Context, ctr, blobName string, offset, count int64) (io.ReadCloser, error) {
	var opts *azblob.DownloadStreamOptions
	if offset > 0 || count > 0 {
		opts = &azblob.DownloadStreamOptions{
			Range: blob.HTTPRange{Offset: offset, Count: count},
		}
	}

	resp, err := c.client.DownloadStream(ctx, ctr, blobName, opts)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *sdkClient) upload(ctx context.Context, ctr, blobName string, body io.Reader, metadata map[string]*string) error {
	_, err := c.client.UploadStream(ctx, ctr, blobName, body, &azblob.UploadStreamOptions{
		Metadata: metadata,
	})
	return err
}

func (c *sdkClient) deleteBlob(ctx context.Context, ctr, blobName string) error {
	_, err := c.client.DeleteBlob(ctx, ctr, blobName, nil)
	return err
}

func (c *sdkClient) setMetadata(ctx context.Context, ctr, blobName string, metadata map[string]*string) error {
	_, err := c.blobClient(ctr, blobName).SetMetadata(ctx, metadata, nil)
	return err
}

func (c *sdkClient) copyBlob(ctx context.Context, ctr, src, dst string) error {
	srcURL := fmt.Sprintf("%s%s/%s", c.client.URL(), ctr, src)
	_, err := c.blobClient(ctr, dst).CopyFromURL(ctx, srcURL, nil)
	return err
}

func (c *sdkClient) listBlobs(ctx context.Context, ctr, prefix, delimiter string, maxResults int32, marker string) (listBlobsResult, error) {
	cc := c.containerClient(ctr)
	var result listBlobsResult

	if delimiter == "" {
		opts := &container.ListBlobsFlatOptions{
			MaxResults: &maxResults,
		}
		if prefix != "" {
			opts.Prefix = &prefix
		}
		if marker != "" {
			opts.Marker = &marker
		}

		pager := cc.NewListBlobsFlatPager(opts)
		if !pager.More() {
			return result, nil
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return listBlobsResult{}, err
		}

		for _, item := range page.Segment.BlobItems {
			result.items = append(result.items, blobItem{
				name:          derefString(item.Name),
				contentLength: derefInt64(item.Properties.ContentLength),
				lastModified:  derefTime(item.Properties.LastModified),
				metadata:      item.Metadata,
			})
		}
		if page.NextMarker != nil && *page.NextMarker != "" {
			result.nextMarker = *page.NextMarker
		}
	} else {
		opts := &container.ListBlobsHierarchyOptions{
			MaxResults: &maxResults,
		}
		if prefix != "" {
			opts.Prefix = &prefix
		}
		if marker != "" {
			opts.Marker = &marker
		}

		pager := cc.NewListBlobsHierarchyPager(delimiter, opts)
		if !pager.More() {
			return result, nil
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			return listBlobsResult{}, err
		}

		for _, item := range page.Segment.BlobItems {
			result.items = append(result.items, blobItem{
				name:          derefString(item.Name),
				contentLength: derefInt64(item.Properties.ContentLength),
				lastModified:  derefTime(item.Properties.LastModified),
				metadata:      item.Metadata,
			})
		}
		for _, p := range page.Segment.BlobPrefixes {
			result.prefixes = append(result.prefixes, derefString(p.Name))
		}
		if page.NextMarker != nil && *page.NextMarker != "" {
			result.nextMarker = *page.NextMarker
		}
	}
	return result, nil
}

func (c *sdkClient) signedURL(ctr, blobName string, method string, expiry time.Time) (string, error) {
	if c.sharedKey == nil {
		return "", ErrSignedURLRequiresSharedKey
	}

	perms := sas.BlobPermissions{}
	switch method {
	case "GET":
		perms.Read = true
	case "PUT":
		perms.Write = true
		perms.Create = true
	}

	qp, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    expiry,
		Permissions:   perms.String(),
		ContainerName: ctr,
		BlobName:      blobName,
	}.SignWithSharedKey(c.sharedKey)
	if err != nil {
		return "", fmt.Errorf("azure: sign SAS: %w", err)
	}

	return fmt.Sprintf("%s%s/%s?%s", c.client.URL(), ctr, blobName, qp.Encode()), nil
}

// --- helpers ---

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

func toAzureMetadata(md map[string]string) map[string]*string {
	if md == nil {
		return nil
	}
	out := make(map[string]*string, len(md))
	for k, v := range md {
		v := v
		out[k] = &v
	}
	return out
}

func fromAzureMetadata(md map[string]*string) map[string]string {
	if md == nil {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if v != nil {
			out[k] = *v
		}
	}
	return out
}