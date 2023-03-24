#ifndef AQP_H
#define AQP_H

#include <postgres.h>

/*
 * XXX(zy): We must build against the customized PG 13.1 with AB-tree.
 * TODO(zy): The plan is to move all additional support out of PG 13.1 and
 * support higher PG versions.
 */
#ifndef HAS_BUILTIN_ABTREE_SUPPORT
#error  "This extension must be compiled against PG 13.1 with builtin AB-tree support"
#endif

#include <fmgr.h>

/*
 * TODO(zy) this is fixed to aqp right now, which makes the module not
 * relocatable. Does it make sense to make this relocatable in the future?
 */
#define AQP_SCHEMA "aqp"

#endif      /* AQP_H */

