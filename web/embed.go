package web

import "embed"

// StaticFS embeds all static assets (HTML, CSS, JS) into the compiled Go binary
//
//go:embed static/*
var StaticFS embed.FS
