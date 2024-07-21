// Global database config.
package config

// Name of the database.
const DBName = "bumble"

// Prompt printed by REPL.
const Prompt = DBName + "> "

// Number of pages.
const NumPages = 32

// Name of log file.
const LogFileName = "./db.log"

// Return prompt if requested, else "".
func GetPrompt(flag bool) string {
	if flag {
		return Prompt
	}
	return ""
}
