module github.com/nhatminh06/matchsense/match-simulator

go 1.25.1

replace github.com/nhatminh06/matchsense/common => ../common

require github.com/nhatminh06/matchsense/common v0.0.0

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
)
