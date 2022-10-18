/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __MOBJECT_STORE
#define __MOBJECT_STORE

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include <time.h>

#include <mobject-client.h>

// derived from: http://docs.ceph.com/docs/master/rados/api/librados/


/* opaque type for a handle for interacting with a mobject store cluster */
typedef struct mobject_store_handle *mobject_store_t;

/**
 * @name Operation Flags
 * Flags for mobject_store_read_op_operate(), mobject_store_write_op_operate(),
 * mobject_store_aio_read_op_operate(), and mobject_store_aio_write_op_operate().
 * See libmobject_store.hpp for details.
 * @{
 */
enum {
  LIBMOBJECT_OPERATION_NOFLAG             = 0,
  LIBMOBJECT_OPERATION_BALANCE_READS      = 1,
  LIBMOBJECT_OPERATION_LOCALIZE_READS     = 2,
  LIBMOBJECT_OPERATION_ORDER_READS_WRITES = 4,
  LIBMOBJECT_OPERATION_IGNORE_CACHE       = 8,
  LIBMOBJECT_OPERATION_SKIPRWLOCKS        = 16,
  LIBMOBJECT_OPERATION_IGNORE_OVERLAY     = 32,
  /* send requests to cluster despite the cluster or pool being marked
     full; ops will either succeed (e.g., delete) or return EDQUOT or
     ENOSPC. */
  LIBMOBJECT_OPERATION_FULL_TRY           = 64,
  /*
   * Mainly for delete op
   */
  LIBMOBJECT_OPERATION_FULL_FORCE		= 128,
  LIBMOBJECT_OPERATION_IGNORE_REDIRECT	= 256,
};
/** @} */

#define LIBMOBJECT_CREATE_EXCLUSIVE 1
#define LIBMOBJECT_CREATE_IDEMPOTENT 0

/**
 * @typedef mobject_store_ioctx_t
 *
 * An io context encapsulates a few settings for all I/O operations
 * done on it:
 * - pool - set when the io context is created (see mobject_store_ioctx_create())
 * - snapshot context for writes (see
 *   mobject_store_ioctx_selfmanaged_snap_set_write_ctx())
 * - snapshot id to read from (see mobject_store_ioctx_snap_set_read())
 * - object locator for all single-object operations (see
 *   mobject_store_ioctx_locator_set_key())
 * - namespace for all single-object operations (see
 *   mobject_store_ioctx_set_namespace()).  Set to LIBMOBJECT_ALL_NSPACES
 *   before mobject_store_nobjects_list_open() will list all objects in all
 *   namespaces.
 *
 * @warning Changing any of these settings is not thread-safe -
 * libmobject_store users must synchronize any of these changes on their own,
 * or use separate io contexts for each thread
 */
typedef struct mobject_store_ioctx *mobject_store_ioctx_t;

/**
 * @typedef mobject_store_omap_iter_t
 * An iterator for listing omap key/value pairs on an object.
 * Used with mobject_store_read_op_omap_get_keys(), mobject_store_read_op_omap_get_vals(),
 * mobject_store_read_op_omap_get_vals_by_keys(), mobject_store_omap_get_next(), and
 * mobject_store_omap_get_end().
 * 
 * NOW DECLARED in mobject-client.h
 */

/**
 * @typedef mobject_store_write_op_t
 *
 * An object write operation stores a number of operations which can be
 * executed atomically. For usage, see:
 * - Creation and deletion: mobject_store_create_write_op() mobject_store_release_write_op()
 * - Extended attribute manipulation: mobject_store_write_op_cmpxattr()
 *   mobject_store_write_op_cmpxattr(), mobject_store_write_op_setxattr(),
 *   mobject_store_write_op_rmxattr()
 * - Object map key/value pairs: mobject_store_write_op_omap_set(),
 *   mobject_store_write_op_omap_rm_keys(), mobject_store_write_op_omap_clear(),
 *   mobject_store_write_op_omap_cmp()
 * - Object properties: mobject_store_write_op_assert_exists(),
 *   mobject_store_write_op_assert_version()
 * - Creating objects: mobject_store_write_op_create()
 * - IO on objects: mobject_store_write_op_append(), mobject_store_write_op_write(), mobject_store_write_op_zero
 *   mobject_store_write_op_write_full(), mobject_store_write_op_writesame(), mobject_store_write_op_remove,
 *   mobject_store_write_op_truncate(), mobject_store_write_op_zero(), mobject_store_write_op_cmpext()
 * - Hints: mobject_store_write_op_set_alloc_hint()
 * - Performing the operation: mobject_store_write_op_operate(), mobject_store_aio_write_op_operate()
 *
 * NOW DECLARED in mobject-client.h
 */

/**
 * @typedef mobject_store_read_op_t
 *
 * An object read operation stores a number of operations which can be
 * executed atomically. For usage, see:
 * - Creation and deletion: mobject_store_create_read_op() mobject_store_release_read_op()
 * - Extended attribute manipulation: mobject_store_read_op_cmpxattr(),
 *   mobject_store_read_op_getxattr(), mobject_store_read_op_getxattrs()
 * - Object map key/value pairs: mobject_store_read_op_omap_get_vals(),
 *   mobject_store_read_op_omap_get_keys(), mobject_store_read_op_omap_get_vals_by_keys(),
 *   mobject_store_read_op_omap_cmp()
 * - Object properties: mobject_store_read_op_stat(), mobject_store_read_op_assert_exists(),
 *   mobject_store_read_op_assert_version()
 * - IO on objects: mobject_store_read_op_read(), mobject_store_read_op_checksum(),
 *   mobject_store_read_op_cmpext()
 * - Custom operations: mobject_store_read_op_exec(), mobject_store_read_op_exec_user_buf()
 * - Request properties: mobject_store_read_op_set_flags()
 * - Performing the operation: mobject_store_read_op_operate(),
 *   mobject_store_aio_read_op_operate()
 * NOW DECLARED in mobject-client.h
 */

/**
 * @typedef mobject_store_completion_t
 * Represents the state of an asynchronous operation - it contains the
 * return value once the operation completes, and can be used to block
 * until the operation is complete or safe.
 */
typedef struct mobject_store_completion* mobject_store_completion_t;

#define MOBJECT_COMPLETION_NULL ((mobject_store_completion_t)0)

/*****************************************
 * mobject store setup/teardown routines *
 *****************************************/

/**
 * Create a handle to a mobject cluster.
 *
 * @param[in/out] cluster   pointer to store mobject cluster handle at
 * @param[in]          id   the user to connect as (NOTE: currently ignored in mobject store)
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_create(
    mobject_store_t *cluster,
    const char * const id);

static inline
int mobject_store_conf_read_file(
    mobject_store_t cluster,
    const char * path)
{
    return 0;
}

/**
 * Connect to a mobject cluster.
 *
 * @param[in] cluster   handle to mobject cluster
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_connect(mobject_store_t cluster);

/**
 * Disconnects from a mobject cluster and destroys the cluster handle.
 * 
 * @param[in] cluster   handle to mobject cluster
 */
void mobject_store_shutdown(mobject_store_t cluster);

/**********************************************
 * mobject store pool setup/teardown routines *
 **********************************************/

/**
 * Creates a new storage pool.
 *
 * @param[in] cluster       handle to mobject cluster
 * @param[in] pool_name     the name of the pool
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_pool_create(
    mobject_store_t cluster,
    const char * pool_name);

/**
 * Deletes a storage pool and all data stored in it.
 *
 * @param[in] cluster       handle to mobject cluster
 * @param[in] pool_name     the name of the pool
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_pool_delete(
    mobject_store_t cluster,
    const char * pool_name);


/*****************************************************
 * mobject store i/o context setup/teardown routines *
 *****************************************************/

/**
 * Create an I/O context for a mobject cluster.
 *
 * @param[in] cluster       handle to mobject cluster
 * @param[in] pool_name     name of the pool
 * @param[in/out] ioctx     pointer to store the io context at
 * @returns 0 on success, negative error code on failure
 *
 * NOTES:
 *      - only one global pool exists and is defined at the top of this header
 */
int mobject_store_ioctx_create(
    mobject_store_t cluster,
    const char * pool_name,
    mobject_store_ioctx_t *ioctx);

/**
 * Destroy an I/O context.
 *
 * @param[in] ioctx     io context to destroy
 */
void mobject_store_ioctx_destroy(mobject_store_ioctx_t ioctx);

/******************************
 * mobject store i/o routines *
 ******************************/

/**
 * Create a new mobject_store_write_op_t write operation. This will store all actions
 * to be performed atomically. You must call mobject_store_release_write_op when you are
 * finished with it.
 *
 * @returns non-NULL on success, NULL on memory allocation error.
 */
mobject_store_write_op_t mobject_store_create_write_op(void);

/**
 * Free a mobject_store_write_op_t, must be called when you're done with it.
 * @param write_op operation to deallocate, created with mobject_store_create_write_op
 */
void mobject_store_release_write_op(mobject_store_write_op_t write_op);

/**
 * Create the object
 * @param write_op operation to add this action to
 * @param exclusive set to either LIBMOBJECT_CREATE_EXCLUSIVE or
   LIBMOBJECT_CREATE_IDEMPOTENT
 * will error if the object already exists.
 * @param category category string (DEPRECATED, HAS NO EFFECT)
 */
void mobject_store_write_op_create(mobject_store_write_op_t write_op,
                                   int exclusive,
                                   const char* category);

/**
 * Write to offset
 * @param write_op operation to add this action to
 * @param offset offset to write to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void mobject_store_write_op_write(mobject_store_write_op_t write_op,
                                  const char *buffer,
                                  size_t len,
                                  uint64_t offset);

/**
 * Write whole object, atomically replacing it.
 * @param write_op operation to add this action to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void mobject_store_write_op_write_full(mobject_store_write_op_t write_op,
                                       const char *buffer,
                                       size_t len);

/**
 * Write the same buffer multiple times
 * @param write_op operation to add this action to
 * @param buffer bytes to write
 * @param data_len length of buffer
 * @param write_len total number of bytes to write, as a multiple of @data_len
 * @param offset offset to write to
 */
void mobject_store_write_op_writesame(mobject_store_write_op_t write_op,
                                      const char *buffer,
				      size_t data_len,
				      size_t write_len,
				      uint64_t offset);

/**
 * Append to end of object.
 * @param write_op operation to add this action to
 * @param buffer bytes to write
 * @param len length of buffer
 */
void mobject_store_write_op_append(mobject_store_write_op_t write_op,
		                   const char *buffer,
				   size_t len);
/**
 * Remove object
 * @param write_op operation to add this action to
 */
void mobject_store_write_op_remove(mobject_store_write_op_t write_op);

/**
 * Truncate an object
 * @param write_op operation to add this action to
 * @param offset Offset to truncate to
 */
void mobject_store_write_op_truncate(mobject_store_write_op_t write_op,
                                     uint64_t offset);

/**
 * Zero part of an object
 * @param write_op operation to add this action to
 * @param offset Offset to zero
 * @param len length to zero
 */
void mobject_store_write_op_zero(mobject_store_write_op_t write_op,
                                 uint64_t offset,
                                 uint64_t len);

/**
 * Set key/value pairs on an object
 *
 * @param write_op operation to add this action to
 * @param keys array of null-terminated char arrays representing keys to set
 * @param vals array of pointers to values to set
 * @param lens array of lengths corresponding to each value
 * @param num number of key/value pairs to set
 */
void mobject_store_write_op_omap_set(mobject_store_write_op_t write_op,
                                     char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num);

/**
 * Remove key/value pairs from an object
 *
 * @param write_op operation to add this action to
 * @param keys array of null-terminated char arrays representing keys to remove
 * @param keys_len number of key/value pairs to remove
 */
void mobject_store_write_op_omap_rm_keys(mobject_store_write_op_t write_op,
                                         char const* const* keys,
                                         size_t keys_len);

/**
 * Perform a write operation synchronously
 * @param write_op operation to perform
 * @param io the ioctx that the object is in
 * @param oid the object id
 * @param mtime the time to set the mtime to, NULL for the current time
 * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
 */
int mobject_store_write_op_operate(mobject_store_write_op_t write_op,
                                   mobject_store_ioctx_t io,
				   const char *oid,
				   time_t *mtime,
				   int flags);

/**
 * Perform a write operation asynchronously
 * @param write_op operation to perform
 * @param io the ioctx that the object is in
 * @param completion what to do when operation has been attempted
 * @param oid the object id
 * @param mtime the time to set the mtime to, NULL for the current time
 * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
 */
int mobject_store_aio_write_op_operate(mobject_store_write_op_t write_op,
                                       mobject_store_ioctx_t io,
                                       mobject_store_completion_t completion,
                                       const char *oid,
                                       time_t *mtime,
                                       int flags);

/**
 * Create a new mobject_store_read_op_t write operation. This will store all
 * actions to be performed atomically. You must call
 * mobject_store_release_read_op when you are finished with it (after it
 * completes, or you decide not to send it in the first place).
 *
 * @returns non-NULL on success, NULL on memory allocation error.
 */
mobject_store_read_op_t mobject_store_create_read_op(void);

/**
 * Free a mobject_store_read_op_t, must be called when you're done with it.
 * @param read_op operation to deallocate, created with mobject_store_create_read_op
 */
void mobject_store_release_read_op(mobject_store_read_op_t read_op);

/**
 * Get object size and mtime
 * @param read_op operation to add this action to
 * @param psize where to store object size
 * @param pmtime where to store modification time
 * @param prval where to store the return value of this action
 */
void mobject_store_read_op_stat(mobject_store_read_op_t read_op,
                                uint64_t *psize,
                                time_t *pmtime,
                                int *prval);

/**
 * Read bytes from offset into buffer.
 *
 * prlen will be filled with the number of bytes read if successful.
 * A short read can only occur if the read reaches the end of the
 * object.
 *
 * @param read_op operation to add this action to
 * @param offset offset to read from
 * @param len length of buffer
 * @param buffer where to put the data
 * @param bytes_read where to store the number of bytes read by this action
 * @param prval where to store the return value of this action
 */
void mobject_store_read_op_read(mobject_store_read_op_t read_op,
                                uint64_t offset,
                                size_t len,
                                char *buffer,
                                size_t *bytes_read,
                                int *prval);

/**
 * Start iterating over keys on an object.
 *
 * They will be returned sorted by key, and the iterator
 * will fill in NULL for all values if specified.
 *
 * @param read_op operation to add this action to
 * @param start_after list keys starting after start_after
 * @param max_return list no more than max_return keys
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void mobject_store_read_op_omap_get_keys(mobject_store_read_op_t read_op,
        const char *start_after,
        uint64_t max_return,
        mobject_store_omap_iter_t *iter,
        int *prval);

/**
 * Start iterating over key/value pairs on an object.
 *
 * They will be returned sorted by key.
 *
 * @param read_op operation to add this action to
 * @param start_after list keys starting after start_after
 * @param filter_prefix list only keys beginning with filter_prefix
 * @param max_return list no more than max_return key/value pairs
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void mobject_store_read_op_omap_get_vals(mobject_store_read_op_t read_op,
                                         const char *start_after,
                                         const char *filter_prefix,
                                         uint64_t max_return,
                                         mobject_store_omap_iter_t *iter,
                                         int *prval);

/**
 * Start iterating over specific key/value pairs
 *
 * They will be returned sorted by key.
 *
 * @param read_op operation to add this action to
 * @param keys array of pointers to null-terminated keys to get
 * @param keys_len the number of strings in keys
 * @param iter where to store the iterator
 * @param prval where to store the return value from this action
 */
void mobject_store_read_op_omap_get_vals_by_keys(mobject_store_read_op_t read_op,
                                                 char const* const* keys,
                                                 size_t keys_len,
                                                 mobject_store_omap_iter_t *iter,
                                                 int *prval);

/**
 * Perform a read operation synchronously
 * @param read_op operation to perform
 * @param io the ioctx that the object is in
 * @param oid the object id
 * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
 */
int mobject_store_read_op_operate(mobject_store_read_op_t read_op,
                                  mobject_store_ioctx_t io,
                                  const char *oid,
                                  int flags);

/**
 * Perform a read operation asynchronously
 * @param read_op operation to perform
 * @param io the ioctx that the object is in
 * @param completion what to do when operation has been attempted
 * @param oid the object id
 * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
 */
int mobject_store_aio_read_op_operate(mobject_store_read_op_t read_op,
                                      mobject_store_ioctx_t io,
                                      mobject_store_completion_t completion,
                                      const char *oid,
                                      int flags);

/**
 * Get the next omap key/value pair on the object
 *
 * @pre iter is a valid iterator
 *
 * @post key and val are the next key/value pair. key is
 * null-terminated, and val has length len. If the end of the list has
 * been reached, key and val are NULL, and len is 0. key and val will
 * not be accessible after mobject_store_omap_get_end() is called on iter, so
 * if they are needed after that they should be copied.
 *
 * @param iter iterator to advance
 * @param key where to store the key of the next omap entry
 * @param val where to store the value of the next omap entry
 * @param len where to store the number of bytes in val
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_omap_get_next(mobject_store_omap_iter_t iter,
                                char **key,
                                char **val,
                                size_t *len);

/**
 * Close the omap iterator.
 *
 * iter should not be used after this is called.
 *
 * @param iter the iterator to close
 */
void mobject_store_omap_get_end(mobject_store_omap_iter_t iter);

/**
 * @typedef mobject_store_callback_t
 * Callbacks for asynchrous operations take two parameters:
 * - cb the completion that has finished
 * - arg application defined data made available to the callback function
 */
typedef void (*mobject_store_callback_t)(mobject_store_completion_t cb, void *arg);

/**
 * Constructs a completion to use with asynchronous operations
 *
 * The complete and safe callbacks correspond to operations being
 * acked and committed, respectively. The callbacks are called in
 * order of receipt, so the safe callback may be triggered before the
 * complete callback, and vice versa. This is affected by journalling
 * on the OSDs.
 *
 * @note Read operations only get a complete callback.
 * @note BUG: this should check for ENOMEM instead of throwing an exception
 *
 * @param cb_arg application-defined data passed to the callback functions
 * @param cb_complete the function to be called when the operation is
 * in memory on all relpicas
 * @param cb_safe the function to be called when the operation is on
 * stable storage on all replicas
 * @param pc where to store the completion
 * @returns 0
 */
int mobject_store_aio_create_completion(void *cb_arg,
                                mobject_store_callback_t cb_complete,
                                mobject_store_callback_t cb_safe,
                                mobject_store_completion_t *pc);

/**
 * Block until an operation completes
 *
 * This means it is in memory on all replicas.
 *
 * @note BUG: this should be void
 *
 * @param c operation to wait for
 * @returns 0
 */
int mobject_store_aio_wait_for_complete(mobject_store_completion_t c);

/**
 * Has an asynchronous operation completed?
 *
 * @warning This does not imply that the complete callback has
 * finished
 *
 * @param c async operation to inspect
 * @returns whether c is complete
 */
int mobject_store_aio_is_complete(mobject_store_completion_t c);

/**
 * Get the return value of an asynchronous operation
 *
 * The return value is set when the operation is complete or safe,
 * whichever comes first.
 *
 * @pre The operation is safe or complete
 *
 * @param c async operation to inspect
 * @returns return value of the operation
 */
int mobject_store_aio_get_return_value(mobject_store_completion_t c);

/**
 * Release a completion
 *
 * Call this when you no longer need the completion. It may not be
 * freed immediately if the operation is not acked and committed.
 *
 * @param c completion to release
 */
void mobject_store_aio_release(mobject_store_completion_t c);

#ifdef __cplusplus
}
#endif

#endif /* __MOBJECT_STORE */
