package distdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPut(t *testing.T) {
	db, err := NewDB()
	require.NoError(t, err)

	/* Get a non existing key */
	keyNonExistent := []byte("kNE")
	_, err = db.Get(keyNonExistent)
	require.ErrorIs(t, err, ErrKeyDoesNotExist)

	/* Get-Put a new key-value pair */
	k1, v1 := []byte("key1"), []byte("val1")
	err = db.Put(k1, v1)
	require.NoError(t, err)
	v1FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v1, v1FromDB)

	/* Overwrite an existing val */
	v2 := []byte("val2")
	err = db.Put(k1, v2)
	require.NoError(t, err)
	v2FromDB, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v2, v2FromDB)
}
