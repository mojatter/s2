module github.com/mojatter/s2/s3

go 1.25.0

require (
	github.com/aws/aws-sdk-go-v2 v1.41.11
	github.com/aws/aws-sdk-go-v2/config v1.32.22
	github.com/aws/aws-sdk-go-v2/service/s3 v1.103.1
	github.com/mojatter/s2 v0.11.1
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.12 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.21 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.1.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.31.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.36.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.43.1 // indirect
	github.com/aws/smithy-go v1.27.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/mojatter/s2 => ../
