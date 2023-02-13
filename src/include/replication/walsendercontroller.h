#ifndef _WALSENDERCONTROLLER_H
#define _WALSENDERCONTROLLER_H

#include <signal.h>

#include "fmgr.h"
#include "access/xlogdefs.h"

extern bool am_walsender_controller;

extern bool exec_walsendercontroller_command(const char *query_string);

#endif