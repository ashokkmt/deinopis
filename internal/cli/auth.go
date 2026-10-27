package cli

import (
	"net/http"
	"os"
)

var adminToken string

func SetAdminToken(token string) { adminToken = token }

func setAdminAuth(req *http.Request) {
	token := adminToken
	if token == "" {
		token = os.Getenv("DEINOPIS_ADMIN_TOKEN")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}
