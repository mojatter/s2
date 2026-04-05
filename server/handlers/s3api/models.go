package s3api

import (
	"encoding/xml"
	"time"
)

// ListAllMyBucketsResult represents the XML response for ListBuckets.
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets []Bucket `xml:"Buckets>Bucket"`
}

// Owner represents the owner of a bucket or object.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// Bucket represents a single bucket in ListBuckets.
type Bucket struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

// ListBucketResult represents the XML response for ListObjectsV2.
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []Content      `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
}

// CommonPrefix represents a common prefix in ListObjectsV2.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// Content represents a single object in ListObjectsV2.
type Content struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"` // Quoted string
	Size         uint64    `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
	Owner        *Owner    `xml:"Owner,omitempty"`
}

// CopyObjectResult represents the XML response for CopyObject.
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

// ErrorResponse represents the XML response for S3 errors.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}
