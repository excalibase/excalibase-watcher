package schema

import "os"

func writeFileViaOS(path string, data []byte) error {
	return os.WriteFile(path, data, 0600)
}
