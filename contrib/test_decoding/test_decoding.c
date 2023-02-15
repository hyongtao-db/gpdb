/*-------------------------------------------------------------------------
 *
 * test_decoding.c
 *		  example logical decoding output plugin
 *
 * Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"

#include "replication/logical.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "unistd.h"

#include "cdb/cdbvars.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct
{
	MemoryContext context;
	bool		include_xids;
	bool		include_timestamp;
	bool		skip_empty_xacts;
	bool		xact_wrote_changes;
	bool		only_local;
} TestDecodingData;

static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
							  bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
								ReorderBufferTXN *txn);
static void pg_output_begin(LogicalDecodingContext *ctx,
							TestDecodingData *data,
							ReorderBufferTXN *txn,
							bool last_write);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn, Relation rel,
							 ReorderBufferChange *change);
static void pg_decode_truncate(LogicalDecodingContext *ctx,
							   ReorderBufferTXN *txn,
							   int nrelations, Relation relations[],
							   ReorderBufferChange *change);
static bool pg_decode_filter(LogicalDecodingContext *ctx,
							 RepOriginId origin_id);
static void pg_decode_message(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr message_lsn,
							  bool transactional, const char *prefix,
							  Size sz, const char *message);

static void pg_decode_distributed_forget(LogicalDecodingContext *ctx,
										 DistributedTransactionId gxid, int cnt_segments, int* segment_ids);

void
_PG_init(void)
{
	/* other plugins can perform things here */
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->truncate_cb = pg_decode_truncate;
	cb->commit_cb = pg_decode_commit_txn;
	cb->filter_by_origin_cb = pg_decode_filter;
	cb->shutdown_cb = pg_decode_shutdown;
	cb->message_cb = pg_decode_message;

	cb->distributed_forget_cb = pg_decode_distributed_forget;
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	ListCell   *option;
	TestDecodingData *data;

	data = palloc0(sizeof(TestDecodingData));
	data->context = AllocSetContextCreate(ctx->context,
										  "text conversion context",
										  ALLOCSET_DEFAULT_SIZES);
	data->include_xids = true;
	data->include_timestamp = false;
	data->skip_empty_xacts = false;
	data->only_local = false;

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
	opt->receive_rewrites = false;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-xids") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_xids = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_xids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-timestamp") == 0)
		{
			if (elem->arg == NULL)
				data->include_timestamp = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "force-binary") == 0)
		{
			bool		force_binary;

			if (elem->arg == NULL)
				continue;
			else if (!parse_bool(strVal(elem->arg), &force_binary))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));

			if (force_binary)
				opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
		}
		else if (strcmp(elem->defname, "skip-empty-xacts") == 0)
		{

			if (elem->arg == NULL)
				data->skip_empty_xacts = true;
			else if (!parse_bool(strVal(elem->arg), &data->skip_empty_xacts))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "only-local") == 0)
		{

			if (elem->arg == NULL)
				data->only_local = true;
			else if (!parse_bool(strVal(elem->arg), &data->only_local))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-rewrites") == 0)
		{

			if (elem->arg == NULL)
				continue;
			else if (!parse_bool(strVal(elem->arg), &opt->receive_rewrites))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

/* cleanup this plugin's resources */
static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
	TestDecodingData *data = ctx->output_plugin_private;

	/* cleanup our own resources via memory context reset */
	MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	TestDecodingData *data = ctx->output_plugin_private;

	data->xact_wrote_changes = false;
	if (data->skip_empty_xacts)
		return;

	pg_output_begin(ctx, data, txn, true);
}

static void
pg_output_begin(LogicalDecodingContext *ctx, TestDecodingData *data, ReorderBufferTXN *txn, bool last_write)
{
	OutputPluginPrepareWrite(ctx, last_write);
	if (data->include_xids)
		appendStringInfo(ctx->out, "BEGIN %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "BEGIN");
	OutputPluginWrite(ctx, last_write);
}

/* COMMIT callback */
static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	TestDecodingData *data = ctx->output_plugin_private;

	if (data->skip_empty_xacts && !data->xact_wrote_changes)
		return;

	OutputPluginPrepareWrite(ctx, true);
	if (data->include_xids)
		appendStringInfo(ctx->out, "COMMIT %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "COMMIT");

	if(txn->is_one_phase)
	{
		appendStringInfo(ctx->out, " ONE_PHASE");
	}

	appendStringInfo(ctx->out, " gxid:%ld", txn->gxid);
	appendStringInfo(ctx->out, " segmentid:%d", GpIdentity.segindex);

	if (data->include_timestamp)
		appendStringInfo(ctx->out, " (at %s)",
						 timestamptz_to_str(txn->commit_time));

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	fprintf(f, "commit data len:%d\n", ctx->out->len);
	for(int i = 0; i < ctx->out->len; ++i)
	{
		fprintf(f, "%c", ctx->out->data[i]);
	}
	fprintf(f, "\n");
	fclose(f);

	OutputPluginWrite(ctx, true);
}

static void pg_decode_distributed_forget(LogicalDecodingContext *ctx,
										 DistributedTransactionId gxid, int cnt_segments, int* segment_ids)
{
	TestDecodingData *data = ctx->output_plugin_private;

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	//fprintf(f, "%d:test_decoding, call back:%ld\n", getpid(), gxid);

	//这两行可别对我们造成什么影响
	data->xact_wrote_changes = false;
	if (data->skip_empty_xacts)
		return;

	//fprintf(f, "%d:test_decoding, pass the return:%ld\n", getpid(), gxid);

	//这里我就跟上边保持一样了，理论上不会有
	OutputPluginPrepareWrite(ctx, true);//推测2参是本次调用这里是否是最后一写。
	
	appendStringInfo(ctx->out, "DISTRIBUTED FORGET %ld ", gxid);
	//appendStringInfo(ctx->out, " count of segments:%d", cnt_segments);
	
	fprintf(f, "segment cnt:%d\n", cnt_segments);
	//fprintf(f, "segment value:%d\n", segment_ids[0]);
	
	for(int i = 0; i < cnt_segments-1; ++i)
	{
		appendStringInfo(ctx->out, "segment %d,", segment_ids[i]);
	}
	appendStringInfo(ctx->out, "segment %d", segment_ids[cnt_segments-1]);
	

	for(int i = 0; i < ctx->out->len; ++i)
	{
		fprintf(f, "%c", ctx->out->data[i]);
	}
	fprintf(f, "\n");
	//fprintf(f, "%s\n", ctx->out->data);

	OutputPluginWrite(ctx, true);

	fclose(f);
}

static bool
pg_decode_filter(LogicalDecodingContext *ctx,
				 RepOriginId origin_id)
{
	TestDecodingData *data = ctx->output_plugin_private;

	if (data->only_local && origin_id != InvalidRepOriginId)
		return true;
	return false;
}

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if standard_conforming_strings were enabled.
 */
static void
print_literal(StringInfo s, Oid typid, char *outputstr)
{
	const char *valptr;

	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			/* NB: We don't care about Inf, NaN et al. */
			appendStringInfoString(s, outputstr);
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(s, "B'%s'", outputstr);
			break;

		case BOOLOID:
			if (strcmp(outputstr, "t") == 0)
				appendStringInfoString(s, "true");
			else
				appendStringInfoString(s, "false");
			break;

		default:
			appendStringInfoChar(s, '\'');
			for (valptr = outputstr; *valptr; valptr++)
			{
				char		ch = *valptr;

				if (SQL_STR_DOUBLE(ch, false))
					appendStringInfoChar(s, ch);
				appendStringInfoChar(s, ch);
			}
			appendStringInfoChar(s, '\'');
			break;
	}
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_kafka_value(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
	int			natt;

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		bool		isnull;		/* column is null? */

		attr = TupleDescAttr(tupdesc, natt);

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull && skip_nulls)
			continue;

		/* print attribute name */
		appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));

		/* print attribute type */
		appendStringInfoString(s, "<*>");
		appendStringInfoString(s, format_type_be(typid));
		appendStringInfoString(s, "<*>");

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		/* print data */
		if (isnull)
			appendStringInfoString(s, "null");
		else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
			appendStringInfoString(s, "unchanged-toast-datum");
		else if (!typisvarlena)
			print_literal(s, typid,
						  OidOutputFunctionCall(typoutput, origval));
		else
		{
			Datum		val;	/* definitely detoasted Datum */

			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
			print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
		}
		appendStringInfoString(s, "<*>");
	}
}

static StringInfo get_delimited_data(Relation relation, ReorderBufferChange *change, ReorderBufferTXN *txn)
{
	StringInfo sendKafkaValue = makeStringInfo();
	Form_pg_class class_form = RelationGetForm(relation);
	TupleDesc tupdesc = RelationGetDescr(relation);

	// append scheme and table info
	appendStringInfoString(sendKafkaValue,
						   quote_qualified_identifier(
													  get_namespace_name(
																		 get_rel_namespace(RelationGetRelid(relation))),
													  class_form->relrewrite ?
													  get_rel_name(class_form->relrewrite) :
													  NameStr(class_form->relname)));
	appendStringInfoString(sendKafkaValue, "<*>");

	// append action
	if (change->action == REORDER_BUFFER_CHANGE_INSERT)
	{
		appendStringInfoString(sendKafkaValue, "insert<*>");
	}
	else if (change->action == REORDER_BUFFER_CHANGE_UPDATE)
	{
		appendStringInfoString(sendKafkaValue, "update<*>");
	}
	else if (change->action == REORDER_BUFFER_CHANGE_DELETE)
	{
		appendStringInfoString(sendKafkaValue, "delete<*>");
	}

	appendStringInfo(sendKafkaValue, "%X", txn->final_lsn);
	appendStringInfoString(sendKafkaValue, "<*>");

	// append data
	if (change->data.tp.newtuple == NULL)
		appendStringInfoString(sendKafkaValue, " (no-tuple-data)");
	else
		tuple_to_kafka_value(sendKafkaValue, tupdesc,
							&change->data.tp.newtuple->tuple,
							false);
	return sendKafkaValue;
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
	int			natt;

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		bool		isnull;		/* column is null? */

		attr = TupleDescAttr(tupdesc, natt);

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull && skip_nulls)
			continue;

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, format_type_be(typid));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		/* print separator */
		appendStringInfoChar(s, ':');

		/* print data */
		if (isnull)
			appendStringInfoString(s, "null");
		else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
			appendStringInfoString(s, "unchanged-toast-datum");
		else if (!typisvarlena)
			print_literal(s, typid,
						  OidOutputFunctionCall(typoutput, origval));
		else
		{
			Datum		val;	/* definitely detoasted Datum */

			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
			print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
		}
	}
}

/*
 * callback for individual changed tuples
 */
static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	TestDecodingData *data;
	Form_pg_class class_form;
	TupleDesc	tupdesc;
	MemoryContext old;

	data = ctx->output_plugin_private;

	/* output BEGIN if we haven't yet */
	if (data->skip_empty_xacts && !data->xact_wrote_changes)
	{
		pg_output_begin(ctx, data, txn, false);
	}
	data->xact_wrote_changes = true;

	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	OutputPluginPrepareWrite(ctx, true);

	appendStringInfo(ctx->out, "gxid:%lld ", change->gxid);
	appendStringInfo(ctx->out, "segmentid:%d ", change->segment_id);

	appendStringInfoString(ctx->out, "table ");
	appendStringInfoString(ctx->out,
						   quote_qualified_identifier(
													  get_namespace_name(
																		 get_rel_namespace(RelationGetRelid(relation))),
													  class_form->relrewrite ?
													  get_rel_name(class_form->relrewrite) :
													  NameStr(class_form->relname)));
	appendStringInfoChar(ctx->out, ':');

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			appendStringInfoString(ctx->out, " INSERT:");
			if (change->data.tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple,
									false);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			appendStringInfoString(ctx->out, " UPDATE:");
			if (change->data.tp.oldtuple != NULL)
			{
				appendStringInfoString(ctx->out, " old-key:");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple,
									true);
				appendStringInfoString(ctx->out, " new-tuple:");
			}

			if (change->data.tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple,
									false);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			appendStringInfoString(ctx->out, " DELETE:");

			/* if there was no PK, we only know that a delete happened */
			if (change->data.tp.oldtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			/* In DELETE, only the replica identity is present; display that */
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple,
									true);
			break;
		default:
			Assert(false);
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

	//StringInfo sendKafkaValue = get_delimited_data(relation, change, txn);

	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");
	for(int i = 0; i < ctx->out->len; ++i)
	{
		fprintf(f, "%c", ctx->out->data[i]);
	}
	fprintf(f, "\n");
	fclose(f);

	OutputPluginWrite(ctx, true);
}

static void
pg_decode_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				   int nrelations, Relation relations[], ReorderBufferChange *change)
{
	TestDecodingData *data;
	MemoryContext old;
	int			i;

	data = ctx->output_plugin_private;

	/* output BEGIN if we haven't yet */
	if (data->skip_empty_xacts && !data->xact_wrote_changes)
	{
		pg_output_begin(ctx, data, txn, false);
	}
	data->xact_wrote_changes = true;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	OutputPluginPrepareWrite(ctx, true);

	appendStringInfoString(ctx->out, "table ");

	for (i = 0; i < nrelations; i++)
	{
		if (i > 0)
			appendStringInfoString(ctx->out, ", ");

		appendStringInfoString(ctx->out,
							   quote_qualified_identifier(get_namespace_name(relations[i]->rd_rel->relnamespace),
														  NameStr(relations[i]->rd_rel->relname)));
	}

	appendStringInfoString(ctx->out, ": TRUNCATE:");

	if (change->data.truncate.restart_seqs
		|| change->data.truncate.cascade)
	{
		if (change->data.truncate.restart_seqs)
			appendStringInfo(ctx->out, " restart_seqs");
		if (change->data.truncate.cascade)
			appendStringInfo(ctx->out, " cascade");
	}
	else
		appendStringInfoString(ctx->out, " (no-flags)");

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

	OutputPluginWrite(ctx, true);
}

static void
pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr lsn, bool transactional,
				  const char *prefix, Size sz, const char *message)
{
	OutputPluginPrepareWrite(ctx, true);
	appendStringInfo(ctx->out, "message: transactional: %d prefix: %s, sz: %zu content:",
					 transactional, prefix, sz);
	appendBinaryStringInfo(ctx->out, message, sz);
	OutputPluginWrite(ctx, true);
}
