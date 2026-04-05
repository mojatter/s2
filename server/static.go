package server

import "embed"

//go:embed static/*
var StaticFS embed.FS
