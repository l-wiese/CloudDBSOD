package metadata;

import java.util.Arrays;
import java.util.List;

//possible domains of an attribute
public class Domains {
	public static List<String> STR=Arrays.asList("varchar","char","longvarchar","bpchar");
	public static List<String> BOOL=Arrays.asList("bit","bool");
	public static List<String> BIGDECIMAL=Arrays.asList("numeric","decimal","balance_t","price_t","value_t","fin_agg_t","s_price_t");
	public static List<String> INT=Arrays.asList("tinyint","smallint","integer","s_qty","int2","int4","s_qty_t");
	public static List<String> LONG=Arrays.asList("bigint","count_t","ident_t","trade_t","s_count_t");
	public static List<String> FLOAT=Arrays.asList("real");
	public static List<String> DOUBLE=Arrays.asList("float","double precision");
	public static List<String> BYTE=Arrays.asList("binary","varbinary","longvarbinary");
	public static List<String> BYTES=Arrays.asList("bytea");
	public static List<String> DATE=Arrays.asList("date");
	public static List<String> TIME=Arrays.asList("time");
	public static List<String> TIMESTAMP=Arrays.asList("timestamp");
}
