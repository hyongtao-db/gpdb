-- first some tests of basic functionality
CREATE EXTENSION plpython2u;

-- really stupid function just to get the module loaded
CREATE FUNCTION stupid() RETURNS text AS 'return "zarkon"' LANGUAGE plpythonu;

select stupid();

-- check 2/3 versioning
CREATE FUNCTION stupidn() RETURNS text AS 'return "zarkon"' LANGUAGE plpython2u;

select stupidn();

-- test multiple arguments and odd characters in function name
CREATE FUNCTION "Argument test #1"(u users, a1 text, a2 text) RETURNS text
	AS
'keys = list(u.keys())
keys.sort()
out = []
for key in keys:
    out.append("%s: %s" % (key, u[key]))
words = a1 + " " + a2 + " => {" + ", ".join(out) + "}"
return words'
	LANGUAGE plpythonu;

select "Argument test #1"(users, fname, lname) from users where lname = 'doe' order by 1;


-- check module contents
CREATE FUNCTION module_contents() RETURNS SETOF text AS
$$
contents = list(filter(lambda x: not x.startswith("__"), dir(plpy)))
contents.sort()
return contents
$$ LANGUAGE plpythonu;

select module_contents();

CREATE FUNCTION elog_test_basic() RETURNS void
AS $$
plpy.debug('debug')
plpy.log('log')
plpy.info('info')
plpy.info(37)
plpy.info()
plpy.info('info', 37, [1, 2, 3])
plpy.notice('notice')
plpy.warning('warning')
plpy.error('error')
$$ LANGUAGE plpythonu;

SELECT elog_test_basic();

SET client_encoding TO 'UTF8';
SET client_min_messages = LOG;

CREATE FUNCTION elog_test_string_truncation() RETURNS void
AS $$
plpy.log(("床前明月光疑是地上霜举头望明月低头思故乡\n"+
"独坐幽篁里弹琴复长啸深林人不知明月来相照\n"+
"千山鸟飞绝万径人踪灭孤舟蓑笠翁独钓寒江雪\n"+
"白日依山尽黄河入海流欲穷千里目更上一层楼\n"+
"好雨知时节当春乃发生随风潜入夜润物细无声\n")*1000)
$$ LANGUAGE plpythonu;

SELECT elog_test_string_truncation();

SET client_min_messages = NOTICE;