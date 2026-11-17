package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCursor(t *testing.T) {
	var _ Cursor = (*FilebasedCursor)(nil)

	f, err := os.CreateTemp(t.TempDir(), "cursor-*")
	assert.NoError(t, err)
	defer f.Close()

	cursor, err := NewFilebasedCursor(f.Name())
	assert.NoError(t, err)

	v, _ := cursor.Get()
	assert.Equal(t, "", v)
	assert.NoError(t, cursor.Set("cursor-0"))
	v, _ = cursor.Get()
	assert.Equal(t, "cursor-0", v)
}
