#include "postgres.h"

#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

#include "access/printtup.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "libpq-int.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/replnodes.h"
#include "pgstat.h"
#include "replication/basebackup.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
//#include "replication/walsender.h"
#include "replication/walsendercontroller.h"
#include "replication/walsender_private.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/faultinjector.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "replication/gp_replication.h"

bool		am_walsender_controller = false;

static StringInfoData output_message;
static StringInfoData reply_message;
static StringInfoData tmpbuf;

#define MAX_SEGMENTS_COUNT 100
PGconn* conns[MAX_SEGMENTS_COUNT + 1];
char* ports[MAX_SEGMENTS_COUNT + 1];
int cnt_conns = 0;

bool CONN_INITED = false;

static void //正常是需要获取地址加port的，这里暂时只处理port
GetPorts()
{
	ports[0] = "15432";

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in GetPorts\n");

	char query[256];
	PGresult* res = NULL;

	const char** keywords;
	const char** values;

	keywords = palloc0((4 + 1) * sizeof(*keywords));
	values = palloc0((4 + 1) * sizeof(*values));
	keywords[0] = "port";
	values[0] = "15432";
	keywords[1] = "hostaddr";
	values[1] = "127.0.0.1";
	keywords[2] = "dbname";
	values[2] = "testdb";
	keywords[3] = "user";
	values[3] = "gpadmin";

	PGconn* conn = PQconnectdbParams(keywords, values, true);
	if (!conn)
	{
		fprintf(f, "connect error1\n");
		fclose(f);
		return;
	}

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(f, "connect error2\n");
		fclose(f);
		return;
	}

	snprintf(query, sizeof(query), "select * from gp_segment_configuration;");
	fprintf(f, "command:%s\n", query);

	res = PQexec(conn, query);
	fprintf(f, "PQexec res:%d\n", PQresultStatus(res));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(f, "exec error\n");
		fclose(f);
		PQfinish(conn);
		return;
	}

	int n = PQntuples(res);
	fprintf(f, "tuple cnt:%d, attrnums:%d\n", n, res->numAttributes);
	cnt_conns = n;

	char* v = NULL;

	for (int i = 1; i < n; i++)
	{
		v = PQgetvalue(res, i, 6);//remember free
		if(v)
		{
			fprintf(f, "%s\n", v);
			//ports[i] = v;
			ports[i] = palloc0(strlen(v) + 1);
			strcpy(ports[i], v);
			fprintf(f, "%s\n", ports[i]);
		}
		else 
			fprintf(f, "null \n");
	}

	PQfinish(conn);
	fclose(f);
}

static void
InitOneConn(int i)
{
	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in InitConn\n");

	const char** keywords;
	const char** values;

	keywords = palloc0((5 + 1) * sizeof(*keywords));
	values = palloc0((5 + 1) * sizeof(*values));
	
	keywords[0] = "port";
	values[0] = ports[i];
	keywords[1] = "hostaddr";
	values[1] = "127.0.0.1";
	keywords[2] = "dbname";
	values[2] = "testdb";
	keywords[3] = "user";
	values[3] = "gpadmin";
	keywords[4] = "replication";
	if(i == 0)values[4] = "master";
	else values[4] = "database";

	conns[i] = PQconnectdbParams(keywords, values, true);
	if (!conns[i])
	{
		fprintf(f, "connect error1, conn num:%d\n", i);
		fclose(f);
		return;
	}

	if (PQstatus(conns[i]) != CONNECTION_OK)
	{
		fprintf(f, "connect error2, conn num:%d\n", i);
		fclose(f);
		return;
	}

	fclose(f);

	return;
}

void InitConns()
{
	if(CONN_INITED)return;

	GetPorts();

	for(int i = 0; i < cnt_conns; ++i)
	{
		InitOneConn(i);
	}

	CONN_INITED = true;
}

static void
DispatchCommand(char* query, ExecStatusType success_ret)
{
	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in DispatchCommand\n");

	PGresult* res = NULL;

	for(int i = 0; i < cnt_conns; ++i)
	{
		res = PQexec(conns[i], query);//注意res空间回收
		fprintf(f, "PQexec res:%d, i:%d\n", PQresultStatus(res), i);

		if (PQresultStatus(res) != success_ret)
		{
			fprintf(f, "exec error:%s\n", PQerrorMessage(conns[i]));
			PQfinish(conns[i]);
			break;
		}
	}

	fclose(f);
}

static void
CreateReplicationSlot(CreateReplicationSlotCmd *cmd)
{
	InitConns();

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in CreateReplicationSlot\n");

	char query[256];

	snprintf(query, sizeof(query), "CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"test_decoding\"",
				 cmd->slotname);
	fprintf(f, "command:%s\n", query);

	DispatchCommand(query, PGRES_TUPLES_OK);

/*
	GpSegConfigEntry *segCnfInfo = NULL;
	segCnfInfo = dbid_get_dbinfo(1);
	if(!segCnfInfo)fprintf(f, "null ptr\n");
	//fprintf(f, "port:%d\n", segCnfInfo->port);
*/
	fclose(f);
}

static void
StartLogicalReplication(StartReplicationCmd *cmd)//理论上我要给controller一个回复的
{
	InitConns();

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in StartLogicalReplication\n");

	char query[256];
	PGresult* res = NULL;

	snprintf(query, sizeof(query), "START_REPLICATION SLOT \"%s\" LOGICAL 0/0", cmd->slotname);
	fprintf(f, "command:%s\n", query);

	DispatchCommand(query, PGRES_COPY_BOTH);

	fclose(f);
}

static void
DropReplicationSlot(DropReplicationSlotCmd *cmd)
{
	InitConns();

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in DropReplicationSlot\n");

	char query[256];
	PGresult* res = NULL;

	snprintf(query, sizeof(query), "DROP_REPLICATION_SLOT %s", cmd->slotname);
	fprintf(f, "command:%s\n", query);

	DispatchCommand(query, PGRES_COMMAND_OK);

	fclose(f);
}

bool
exec_walsendercontroller_command(const char *cmd_string)
{
	int			parse_rc;
	Node	   *cmd_node;
	MemoryContext cmd_context;
	MemoryContext old_context;

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "in exec_walsendercontroller_command:%s\n", cmd_string);

	cmd_context = AllocSetContextCreate(CurrentMemoryContext,
										"Replication command context",
										ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(cmd_context);

	replication_scanner_init(cmd_string);
	parse_rc = replication_yyparse();
	if (parse_rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 (errmsg_internal("replication command parser returned %d",
								  parse_rc))));

	cmd_node = replication_parse_result;

	/*
	 * Allocate buffers that will be used for each outgoing and incoming
	 * message.  We do this just once per command to reduce palloc overhead.
	 */
	initStringInfo(&output_message);//这几个buf做什么用的？
	initStringInfo(&reply_message);
	initStringInfo(&tmpbuf);

	/* Report to pgstat that this process is running */
	pgstat_report_activity(STATE_RUNNING, NULL);

	fprintf(f, "cmd type:%d\n", cmd_node->type);
	switch (cmd_node->type)
	{
		case T_CreateReplicationSlotCmd:
			CreateReplicationSlot((CreateReplicationSlotCmd *) cmd_node);
			break;

		case T_StartReplicationCmd:
			{
				StartReplicationCmd *cmd = (StartReplicationCmd *) cmd_node;
				StartLogicalReplication(cmd);
				break;
			}

		case T_DropReplicationSlotCmd:
			DropReplicationSlot((DropReplicationSlotCmd *) cmd_node);
			break;

		default:
			elog(ERROR, "unrecognized replication command node tag: %u",
				 cmd_node->type);
	}

	/* done */
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(cmd_context);

	/* Send CommandComplete message */
	EndCommand("WangChong is good!!!", DestRemote);

	/* Report to pgstat that this process is now idle */
	pgstat_report_activity(STATE_IDLE, NULL);

	fclose(f);

	return true;
}