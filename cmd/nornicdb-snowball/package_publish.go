package main

import (
	"errors"
	"fmt"
	"os"
)

// publishSnowballPackage installs a completely built and checked library/manifest
// pair. The staging directory shares the destination filesystem. Existing
// artifacts stay available until validation succeeds and are restored if either
// rename fails; failed builds never own the destination files.
func publishSnowballPackage(staged, destination string) (err error) {
	type artifact struct {
		source, target, backup string
		saved, installed       bool
	}
	artifacts := []artifact{
		{source: staged, target: destination, backup: staged + ".previous"},
		{source: manifestPath(staged), target: manifestPath(destination), backup: manifestPath(staged) + ".previous"},
	}
	for _, file := range artifacts {
		info, statErr := os.Lstat(file.target)
		if statErr != nil && !os.IsNotExist(statErr) {
			return statErr
		}
		if statErr == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("package destination is not a regular file: %s", file.target)
		}
	}
	defer func() {
		if err == nil {
			for _, file := range artifacts {
				if file.saved {
					_ = os.Remove(file.backup)
				}
			}
			return
		}
		for i := len(artifacts) - 1; i >= 0; i-- {
			file := &artifacts[i]
			if file.installed {
				err = errors.Join(err, os.Remove(file.target))
			}
			if file.saved {
				if restoreErr := os.Rename(file.backup, file.target); restoreErr != nil {
					err = errors.Join(err, fmt.Errorf("restore %s from %s: %w", file.target, file.backup, restoreErr))
				}
			}
		}
	}()
	for i := range artifacts {
		file := &artifacts[i]
		if renameErr := os.Rename(file.target, file.backup); renameErr == nil {
			file.saved = true
		} else if !os.IsNotExist(renameErr) {
			return renameErr
		}
	}
	for i := range artifacts {
		file := &artifacts[i]
		if err := os.Rename(file.source, file.target); err != nil {
			return err
		}
		file.installed = true
	}
	return nil
}
