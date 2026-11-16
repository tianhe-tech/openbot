package skillgen

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// ResolveConfig fixes paths once for the drafter, service and review commands.
// Reference failures degrade drafting quality, not directory correctness.
func ResolveConfig(cfg Config) (Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return cfg, fmt.Errorf("skillgen: resolve working directory: %w", err)
	}
	if cfg.CandidateDir == "" {
		cfg.CandidateDir = "skills-candidates"
	}
	if cfg.InstallDir == "" {
		cfg.InstallDir = "skills"
	}
	if cfg.ReferenceSkillPath == "" {
		cfg.ReferenceSkillPath = filepath.Join(cfg.InstallDir, "skill-creator", "SKILL.md")
	}
	cfg.CandidateDir, err = resolveDirectory(cfg.CandidateDir, cwd)
	if err != nil {
		return cfg, fmt.Errorf("skillgen: candidate directory: %w", err)
	}
	cfg.InstallDir, err = resolveDirectory(cfg.InstallDir, cwd)
	if err != nil {
		return cfg, fmt.Errorf("skillgen: install directory: %w", err)
	}
	exe, err := os.Executable()
	if err != nil {
		return cfg, fmt.Errorf("skillgen: resolve executable location: %w", err)
	}
	reference, refErr := resolveReferenceFile(cfg.ReferenceSkillPath, cwd, filepath.Dir(exe))
	cfg.ReferenceSkillPath = reference
	if refErr != nil {
		log.Printf("skillgen: reference unavailable; drafting will run without an exemplar: path=%s error=%v", reference, refErr)
	}
	log.Printf("skillgen: resolved paths candidates=%s install=%s reference=%s", cfg.CandidateDir, cfg.InstallDir, cfg.ReferenceSkillPath)
	return cfg, nil
}

func resolveDirectory(path, cwd string) (string, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(cwd, path)
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %s", path)
	}
	return path, nil
}

func resolveReferenceFile(path, cwd, exeDir string) (string, error) {
	candidates := []string{path}
	if !filepath.IsAbs(path) {
		candidates = []string{filepath.Join(cwd, path), filepath.Join(exeDir, path)}
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return candidate, err
		}
		if !info.Mode().IsRegular() {
			return candidate, fmt.Errorf("reference is not a regular file: %s", candidate)
		}
		content, err := os.ReadFile(candidate)
		if err != nil {
			return candidate, err
		}
		if strings.TrimSpace(string(content)) == "" {
			return candidate, fmt.Errorf("reference is empty: %s", candidate)
		}
		return candidate, nil
	}
	return candidates[0], fmt.Errorf("reference not found: %s", path)
}
