package skillgen

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReferenceResolutionPreservesConfiguredPrecedence(t *testing.T) {
	for _, mode := range []string{"cwd", "exe fallback", "directory", "empty", "absolute missing"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			cwd, exe := filepath.Join(root, "cwd"), filepath.Join(root, "bin")
			for _, dir := range []string{cwd, exe} {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
			}
			const ref = "SKILL.md"
			cwdFile, exeFile := filepath.Join(cwd, ref), filepath.Join(exe, ref)
			if err := os.WriteFile(exeFile, []byte("valid exemplar"), 0644); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "cwd":
				if err := os.WriteFile(cwdFile, []byte("cwd exemplar"), 0644); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(cwdFile, 0755); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.WriteFile(cwdFile, []byte(" \n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			input := ref
			if mode == "absolute missing" {
				input = cwdFile
			}
			resolved, err := resolveReferenceFile(input, cwd, exe)
			switch mode {
			case "cwd":
				if err != nil || resolved != cwdFile {
					t.Fatalf("CWD precedence lost: path=%s err=%v", resolved, err)
				}
			case "exe fallback":
				if err != nil || resolved != exeFile {
					t.Fatalf("executable fallback failed: path=%s err=%v", resolved, err)
				}
			default:
				if err == nil || resolved != cwdFile {
					t.Fatalf("invalid explicit path silently relocated: path=%s err=%v", resolved, err)
				}
			}
		})
	}
}

func TestSkillDirectoriesAreCreatedAsAbsoluteDirectories(t *testing.T) {
	root := t.TempDir()
	path, err := resolveDirectory(filepath.Join("new", "skills"), root)
	if err != nil || path != filepath.Join(root, "new", "skills") || !filepath.IsAbs(path) {
		t.Fatalf("new install directory has ambiguous semantics: path=%s err=%v", path, err)
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("install directory not created: %v", err)
	}
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, []byte("file"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveDirectory(file, root); err == nil {
		t.Fatal("ordinary file accepted as install directory")
	}
	cfg, err := ResolveConfig(Config{CandidateDir: filepath.Join(root, "candidates"), InstallDir: path, ReferenceSkillPath: filepath.Join(root, "missing", "SKILL.md")})
	if err != nil || !filepath.IsAbs(cfg.ReferenceSkillPath) || cfg.InstallDir != path {
		t.Fatalf("missing exemplar blocked or relocated valid install config: cfg=%+v err=%v", cfg, err)
	}
}
