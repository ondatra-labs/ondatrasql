// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package browser

import (
	"fmt"
	"os/exec"
	"runtime"
)

// Open opens a URL in the user's default browser.
func Open(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %s: open %s manually", runtime.GOOS, url)
	}
}
