/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "completion.h"
#include "log.h"

int mobject_store_aio_create_completion(void *cb_arg,
                                mobject_store_callback_t cb_complete,
                                mobject_store_callback_t cb_safe,
                                mobject_store_completion_t *pc)
{
	int r;
	mobject_store_completion_t completion = 
		(mobject_store_completion_t)calloc(1, sizeof(struct mobject_store_completion));
	MOBJECT_ASSERT(completion != 0, "Could not allocate mobject_store_completion_t object"); 
	completion->cb_complete   = cb_complete;
	completion->cb_safe       = cb_safe;
	completion->cb_arg        = cb_arg;
	r = ABT_eventual_create(sizeof(int), (void**)(&(completion->eventual)));
	MOBJECT_ASSERT(r == ABT_SUCCESS, "Could not create ABT_eventual");
	completion->ret_value_ptr  = (int*)0;
	r = ABT_rwlock_create(&(completion->lock));
	MOBJECT_ASSERT(r == ABT_SUCCESS, "Could not create ABT_rwlock");

	*pc = completion;
	return 0;
}

int mobject_store_aio_wait_for_complete(mobject_store_completion_t c)
{
	int r;
	if(c == MOBJECT_COMPLETION_NULL) {
		MOBJECT_LOG("Warning: passing NULL to mobject_store_aio_wait_for_complete");
		return -1;
	}

	int* val_ptr = (int*)0;
	r = ABT_eventual_wait(c->eventual, (void**)(&val_ptr));
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_eventual_wait failed");
	r = ABT_rwlock_wrlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_wrlock failed");
	c->ret_value_ptr = val_ptr;
	r = ABT_rwlock_unlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_unlock failed");

	return 0;
}

int mobject_store_aio_is_complete(mobject_store_completion_t c)
{
	int r;
	if(c == MOBJECT_COMPLETION_NULL) {
		MOBJECT_LOG("Warning: passing NULL to mobject_store_aio_is_complete");
		return 0;
	}
	int result = 0;
	r = ABT_rwlock_rdlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_rdlock failed");
	result = (c->ret_value_ptr != (int*)0);
	r = ABT_rwlock_unlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_unlock failed");

	return result;
}

int mobject_store_aio_get_return_value(mobject_store_completion_t c)
{
	int r;
	if(c == MOBJECT_COMPLETION_NULL) {
		MOBJECT_LOG("Warning: passing NULL to mobject_store_aio_get_return_value");
		return 0;
	}
	int result = 0;
	r = ABT_rwlock_rdlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_rdlock failed");
	if(c->ret_value_ptr != (int*)0) result = *(c->ret_value_ptr);
	r = ABT_rwlock_unlock(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_unlock failed");
	return 0;
}

void mobject_store_aio_release(mobject_store_completion_t c)
{
	int r;
	if(c == MOBJECT_COMPLETION_NULL) return;
	r = ABT_eventual_free(c->eventual);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_eventual_free failed");
	r = ABT_rwlock_free(c->lock);
	MOBJECT_ASSERT(r == ABT_SUCCESS, "ABT_rwlock_free failed");
	free(c);
}