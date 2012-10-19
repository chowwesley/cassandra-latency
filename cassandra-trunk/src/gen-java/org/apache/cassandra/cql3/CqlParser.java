// $ANTLR 3.2 Sep 23, 2009 12:02:23 /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g 2012-10-18 18:54:45

    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Collections;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.utils.Pair;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.thrift.InvalidRequestException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "K_USE", "K_SELECT", "K_COUNT", "K_FROM", "K_USING", "K_CONSISTENCY", "K_LEVEL", "K_WHERE", "K_ORDER", "K_BY", "K_LIMIT", "INTEGER", "K_WRITETIME", "K_TTL", "K_AND", "K_ASC", "K_DESC", "K_INSERT", "K_INTO", "K_VALUES", "K_TIMESTAMP", "K_UPDATE", "K_SET", "K_DELETE", "K_BEGIN", "K_BATCH", "K_APPLY", "K_CREATE", "K_KEYSPACE", "K_WITH", "K_COLUMNFAMILY", "K_PRIMARY", "K_KEY", "K_COMPACT", "K_STORAGE", "K_CLUSTERING", "K_INDEX", "IDENT", "K_ON", "K_ALTER", "K_TYPE", "K_ADD", "K_DROP", "K_TRUNCATE", "K_GRANT", "K_TO", "STRING_LITERAL", "K_OPTION", "K_REVOKE", "K_LIST", "K_GRANTS", "K_FOR", "K_DESCRIBE", "K_FULL_ACCESS", "K_NO_ACCESS", "QUOTED_NAME", "K_TOKEN", "UUID", "FLOAT", "QMARK", "K_IN", "K_ASCII", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_COUNTER", "K_DECIMAL", "K_DOUBLE", "K_FLOAT", "K_INT", "K_TEXT", "K_UUID", "K_VARCHAR", "K_VARINT", "K_TIMEUUID", "S", "E", "L", "C", "T", "F", "R", "O", "M", "W", "H", "A", "N", "D", "K", "Y", "I", "U", "P", "G", "K_ALL", "Q", "B", "X", "V", "J", "Z", "DIGIT", "LETTER", "HEX", "WS", "COMMENT", "MULTILINE_COMMENT", "';'", "'('", "')'", "','", "'\\*'", "'='", "'.'", "'+'", "'-'", "':'", "'<'", "'<='", "'>='", "'>'"
    };
    public static final int LETTER=107;
    public static final int K_INT=73;
    public static final int K_CREATE=31;
    public static final int K_CLUSTERING=39;
    public static final int K_WRITETIME=16;
    public static final int EOF=-1;
    public static final int K_PRIMARY=35;
    public static final int K_VALUES=23;
    public static final int K_USE=4;
    public static final int STRING_LITERAL=50;
    public static final int K_GRANT=48;
    public static final int K_ON=42;
    public static final int K_USING=8;
    public static final int K_ADD=45;
    public static final int K_ASC=19;
    public static final int K_KEY=36;
    public static final int COMMENT=110;
    public static final int K_TRUNCATE=47;
    public static final int K_FULL_ACCESS=57;
    public static final int K_ORDER=12;
    public static final int K_ALL=99;
    public static final int D=92;
    public static final int E=80;
    public static final int F=84;
    public static final int G=98;
    public static final int K_COUNT=6;
    public static final int K_KEYSPACE=32;
    public static final int K_TYPE=44;
    public static final int A=90;
    public static final int B=101;
    public static final int C=82;
    public static final int L=81;
    public static final int M=87;
    public static final int N=91;
    public static final int O=86;
    public static final int H=89;
    public static final int I=95;
    public static final int J=104;
    public static final int K_UPDATE=25;
    public static final int K=93;
    public static final int U=96;
    public static final int T=83;
    public static final int W=88;
    public static final int K_TEXT=74;
    public static final int V=103;
    public static final int Q=100;
    public static final int P=97;
    public static final int K_COMPACT=37;
    public static final int S=79;
    public static final int R=85;
    public static final int K_TTL=17;
    public static final int Y=94;
    public static final int X=102;
    public static final int Z=105;
    public static final int T__125=125;
    public static final int K_INDEX=40;
    public static final int K_INSERT=21;
    public static final int WS=109;
    public static final int K_APPLY=30;
    public static final int K_STORAGE=38;
    public static final int K_TIMESTAMP=24;
    public static final int K_AND=18;
    public static final int K_DESC=20;
    public static final int K_TOKEN=60;
    public static final int QMARK=63;
    public static final int K_LEVEL=10;
    public static final int K_BATCH=29;
    public static final int K_UUID=75;
    public static final int K_GRANTS=54;
    public static final int K_ASCII=65;
    public static final int UUID=61;
    public static final int T__118=118;
    public static final int T__119=119;
    public static final int T__116=116;
    public static final int K_LIST=53;
    public static final int K_DELETE=27;
    public static final int T__117=117;
    public static final int K_FOR=55;
    public static final int T__114=114;
    public static final int T__115=115;
    public static final int T__124=124;
    public static final int T__123=123;
    public static final int K_TO=49;
    public static final int T__122=122;
    public static final int K_BY=13;
    public static final int T__121=121;
    public static final int T__120=120;
    public static final int FLOAT=62;
    public static final int K_FLOAT=72;
    public static final int K_VARINT=77;
    public static final int K_DOUBLE=71;
    public static final int K_SELECT=5;
    public static final int K_LIMIT=14;
    public static final int K_ALTER=43;
    public static final int K_BOOLEAN=68;
    public static final int K_SET=26;
    public static final int K_WHERE=11;
    public static final int QUOTED_NAME=59;
    public static final int K_DESCRIBE=56;
    public static final int MULTILINE_COMMENT=111;
    public static final int K_BLOB=67;
    public static final int HEX=108;
    public static final int K_INTO=22;
    public static final int K_REVOKE=52;
    public static final int K_VARCHAR=76;
    public static final int T__113=113;
    public static final int IDENT=41;
    public static final int T__112=112;
    public static final int DIGIT=106;
    public static final int K_BEGIN=28;
    public static final int INTEGER=15;
    public static final int K_COUNTER=69;
    public static final int K_DECIMAL=70;
    public static final int K_CONSISTENCY=9;
    public static final int K_WITH=33;
    public static final int K_IN=64;
    public static final int K_NO_ACCESS=58;
    public static final int K_FROM=7;
    public static final int K_COLUMNFAMILY=34;
    public static final int K_OPTION=51;
    public static final int K_DROP=46;
    public static final int K_BIGINT=66;
    public static final int K_TIMEUUID=78;

    // delegates
    // delegators


        public CqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return CqlParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g"; }


        private List<String> recognitionErrors = new ArrayList<String>();
        private int currentBindMarkerIdx = -1;

        public void displayRecognitionError(String[] tokenNames, RecognitionException e)
        {
            String hdr = getErrorHeader(e);
            String msg = getErrorMessage(e, tokenNames);
            recognitionErrors.add(hdr + " " + msg);
        }

        public void addRecognitionError(String msg)
        {
            recognitionErrors.add(msg);
        }

        public List<String> getRecognitionErrors()
        {
            return recognitionErrors;
        }

        public void throwLastRecognitionError() throws InvalidRequestException
        {
            if (recognitionErrors.size() > 0)
                throw new InvalidRequestException(recognitionErrors.get((recognitionErrors.size()-1)));
        }

        // used by UPDATE of the counter columns to validate if '-' was supplied by user
        public void validateMinusSupplied(Object op, final Term value, IntStream stream) throws MissingTokenException
        {
            if (op == null && (value.isBindMarker() || Long.parseLong(value.getText()) > 0))
                throw new MissingTokenException(102, stream, value);
        }



    // $ANTLR start "query"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:122:1: query returns [ParsedStatement stmnt] : st= cqlStatement ( ';' )* EOF ;
    public final ParsedStatement query() throws RecognitionException {
        ParsedStatement stmnt = null;

        ParsedStatement st = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:125:5: (st= cqlStatement ( ';' )* EOF )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:125:7: st= cqlStatement ( ';' )* EOF
            {
            pushFollow(FOLLOW_cqlStatement_in_query72);
            st=cqlStatement();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:125:23: ( ';' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==112) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:125:24: ';'
            	    {
            	    match(input,112,FOLLOW_112_in_query75); 

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            match(input,EOF,FOLLOW_EOF_in_query79); 
             stmnt = st; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmnt;
    }
    // $ANTLR end "query"


    // $ANTLR start "cqlStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:128:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= grantStatement | st16= revokeStatement | st17= listGrantsStatement | st18= alterKeyspaceStatement );
    public final ParsedStatement cqlStatement() throws RecognitionException {
        ParsedStatement stmt = null;

        SelectStatement.RawStatement st1 = null;

        UpdateStatement st2 = null;

        UpdateStatement st3 = null;

        BatchStatement st4 = null;

        DeleteStatement st5 = null;

        UseStatement st6 = null;

        TruncateStatement st7 = null;

        CreateKeyspaceStatement st8 = null;

        CreateColumnFamilyStatement.RawStatement st9 = null;

        CreateIndexStatement st10 = null;

        DropKeyspaceStatement st11 = null;

        DropColumnFamilyStatement st12 = null;

        DropIndexStatement st13 = null;

        AlterTableStatement st14 = null;

        GrantStatement st15 = null;

        RevokeStatement st16 = null;

        ListGrantsStatement st17 = null;

        AlterKeyspaceStatement st18 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:130:5: (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= grantStatement | st16= revokeStatement | st17= listGrantsStatement | st18= alterKeyspaceStatement )
            int alt2=18;
            alt2 = dfa2.predict(input);
            switch (alt2) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:130:7: st1= selectStatement
                    {
                    pushFollow(FOLLOW_selectStatement_in_cqlStatement113);
                    st1=selectStatement();

                    state._fsp--;

                     stmt = st1; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:131:7: st2= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_cqlStatement138);
                    st2=insertStatement();

                    state._fsp--;

                     stmt = st2; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:132:7: st3= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_cqlStatement163);
                    st3=updateStatement();

                    state._fsp--;

                     stmt = st3; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:133:7: st4= batchStatement
                    {
                    pushFollow(FOLLOW_batchStatement_in_cqlStatement188);
                    st4=batchStatement();

                    state._fsp--;

                     stmt = st4; 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:134:7: st5= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_cqlStatement214);
                    st5=deleteStatement();

                    state._fsp--;

                     stmt = st5; 

                    }
                    break;
                case 6 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:135:7: st6= useStatement
                    {
                    pushFollow(FOLLOW_useStatement_in_cqlStatement239);
                    st6=useStatement();

                    state._fsp--;

                     stmt = st6; 

                    }
                    break;
                case 7 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:136:7: st7= truncateStatement
                    {
                    pushFollow(FOLLOW_truncateStatement_in_cqlStatement267);
                    st7=truncateStatement();

                    state._fsp--;

                     stmt = st7; 

                    }
                    break;
                case 8 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:137:7: st8= createKeyspaceStatement
                    {
                    pushFollow(FOLLOW_createKeyspaceStatement_in_cqlStatement290);
                    st8=createKeyspaceStatement();

                    state._fsp--;

                     stmt = st8; 

                    }
                    break;
                case 9 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:138:7: st9= createColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_createColumnFamilyStatement_in_cqlStatement307);
                    st9=createColumnFamilyStatement();

                    state._fsp--;

                     stmt = st9; 

                    }
                    break;
                case 10 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:139:7: st10= createIndexStatement
                    {
                    pushFollow(FOLLOW_createIndexStatement_in_cqlStatement319);
                    st10=createIndexStatement();

                    state._fsp--;

                     stmt = st10; 

                    }
                    break;
                case 11 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:140:7: st11= dropKeyspaceStatement
                    {
                    pushFollow(FOLLOW_dropKeyspaceStatement_in_cqlStatement338);
                    st11=dropKeyspaceStatement();

                    state._fsp--;

                     stmt = st11; 

                    }
                    break;
                case 12 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:141:7: st12= dropColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_dropColumnFamilyStatement_in_cqlStatement356);
                    st12=dropColumnFamilyStatement();

                    state._fsp--;

                     stmt = st12; 

                    }
                    break;
                case 13 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:142:7: st13= dropIndexStatement
                    {
                    pushFollow(FOLLOW_dropIndexStatement_in_cqlStatement370);
                    st13=dropIndexStatement();

                    state._fsp--;

                     stmt = st13; 

                    }
                    break;
                case 14 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:143:7: st14= alterTableStatement
                    {
                    pushFollow(FOLLOW_alterTableStatement_in_cqlStatement391);
                    st14=alterTableStatement();

                    state._fsp--;

                     stmt = st14; 

                    }
                    break;
                case 15 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:144:7: st15= grantStatement
                    {
                    pushFollow(FOLLOW_grantStatement_in_cqlStatement411);
                    st15=grantStatement();

                    state._fsp--;

                     stmt = st15; 

                    }
                    break;
                case 16 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:145:7: st16= revokeStatement
                    {
                    pushFollow(FOLLOW_revokeStatement_in_cqlStatement436);
                    st16=revokeStatement();

                    state._fsp--;

                     stmt = st16; 

                    }
                    break;
                case 17 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:146:7: st17= listGrantsStatement
                    {
                    pushFollow(FOLLOW_listGrantsStatement_in_cqlStatement460);
                    st17=listGrantsStatement();

                    state._fsp--;

                     stmt = st17; 

                    }
                    break;
                case 18 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:147:7: st18= alterKeyspaceStatement
                    {
                    pushFollow(FOLLOW_alterKeyspaceStatement_in_cqlStatement480);
                    st18=alterKeyspaceStatement();

                    state._fsp--;

                     stmt = st18; 

                    }
                    break;

            }
             if (stmt != null) stmt.setBoundTerms(currentBindMarkerIdx + 1); 
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "cqlStatement"


    // $ANTLR start "useStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:153:1: useStatement returns [UseStatement stmt] : K_USE ks= keyspaceName ;
    public final UseStatement useStatement() throws RecognitionException {
        UseStatement stmt = null;

        String ks = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:154:5: ( K_USE ks= keyspaceName )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:154:7: K_USE ks= keyspaceName
            {
            match(input,K_USE,FOLLOW_K_USE_in_useStatement510); 
            pushFollow(FOLLOW_keyspaceName_in_useStatement514);
            ks=keyspaceName();

            state._fsp--;

             stmt = new UseStatement(ks); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "useStatement"


    // $ANTLR start "selectStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:157:1: selectStatement returns [SelectStatement.RawStatement expr] : K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )? ;
    public final SelectStatement.RawStatement selectStatement() throws RecognitionException {
        SelectStatement.RawStatement expr = null;

        Token rows=null;
        Token K_LEVEL1=null;
        List<Selector> sclause = null;

        CFName cf = null;

        List<Relation> wclause = null;



                boolean isCount = false;
                ConsistencyLevel cLevel = ConsistencyLevel.ONE;
                int limit = 10000;
                Map<ColumnIdentifier, Boolean> orderings = new LinkedHashMap<ColumnIdentifier, Boolean>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:5: ( K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )? )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:7: K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )?
            {
            match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement548); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:16: (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( ((LA3_0>=K_CONSISTENCY && LA3_0<=K_LEVEL)||(LA3_0>=K_WRITETIME && LA3_0<=K_TTL)||(LA3_0>=K_VALUES && LA3_0<=K_TIMESTAMP)||(LA3_0>=K_KEY && LA3_0<=K_CLUSTERING)||LA3_0==IDENT||LA3_0==K_TYPE||LA3_0==QUOTED_NAME||(LA3_0>=K_ASCII && LA3_0<=K_TIMEUUID)||LA3_0==116) ) {
                alt3=1;
            }
            else if ( (LA3_0==K_COUNT) ) {
                int LA3_2 = input.LA(2);

                if ( (LA3_2==113) ) {
                    alt3=2;
                }
                else if ( (LA3_2==K_FROM||LA3_2==115) ) {
                    alt3=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 3, 2, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:18: sclause= selectClause
                    {
                    pushFollow(FOLLOW_selectClause_in_selectStatement554);
                    sclause=selectClause();

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:41: ( K_COUNT '(' sclause= selectCountClause ')' )
                    {
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:41: ( K_COUNT '(' sclause= selectCountClause ')' )
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:171:42: K_COUNT '(' sclause= selectCountClause ')'
                    {
                    match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement559); 
                    match(input,113,FOLLOW_113_in_selectStatement561); 
                    pushFollow(FOLLOW_selectCountClause_in_selectStatement565);
                    sclause=selectCountClause();

                    state._fsp--;

                    match(input,114,FOLLOW_114_in_selectStatement567); 
                     isCount = true; 

                    }


                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement580); 
            pushFollow(FOLLOW_columnFamilyName_in_selectStatement584);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:173:7: ( K_USING K_CONSISTENCY K_LEVEL )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==K_USING) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:173:9: K_USING K_CONSISTENCY K_LEVEL
                    {
                    match(input,K_USING,FOLLOW_K_USING_in_selectStatement594); 
                    match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_selectStatement596); 
                    K_LEVEL1=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_selectStatement598); 
                     cLevel = ConsistencyLevel.valueOf((K_LEVEL1!=null?K_LEVEL1.getText():null).toUpperCase()); 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:174:7: ( K_WHERE wclause= whereClause )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==K_WHERE) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:174:9: K_WHERE wclause= whereClause
                    {
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement613); 
                    pushFollow(FOLLOW_whereClause_in_selectStatement617);
                    wclause=whereClause();

                    state._fsp--;


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:175:7: ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==K_ORDER) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:175:9: K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )*
                    {
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_selectStatement630); 
                    match(input,K_BY,FOLLOW_K_BY_in_selectStatement632); 
                    pushFollow(FOLLOW_orderByClause_in_selectStatement634);
                    orderByClause(orderings);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:175:47: ( ',' orderByClause[orderings] )*
                    loop6:
                    do {
                        int alt6=2;
                        int LA6_0 = input.LA(1);

                        if ( (LA6_0==115) ) {
                            alt6=1;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:175:49: ',' orderByClause[orderings]
                    	    {
                    	    match(input,115,FOLLOW_115_in_selectStatement639); 
                    	    pushFollow(FOLLOW_orderByClause_in_selectStatement641);
                    	    orderByClause(orderings);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop6;
                        }
                    } while (true);


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:176:7: ( K_LIMIT rows= INTEGER )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==K_LIMIT) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:176:9: K_LIMIT rows= INTEGER
                    {
                    match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement658); 
                    rows=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectStatement662); 
                     limit = Integer.parseInt((rows!=null?rows.getText():null)); 

                    }
                    break;

            }


                      SelectStatement.Parameters params = new SelectStatement.Parameters(cLevel,
                                                                                         limit,
                                                                                         orderings,
                                                                                         isCount);
                      expr = new SelectStatement.RawStatement(cf, params, sclause, wclause);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectStatement"


    // $ANTLR start "selectClause"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:186:1: selectClause returns [List<Selector> expr] : (t1= selector ( ',' tN= selector )* | '\\*' );
    public final List<Selector> selectClause() throws RecognitionException {
        List<Selector> expr = null;

        Selector t1 = null;

        Selector tN = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:187:5: (t1= selector ( ',' tN= selector )* | '\\*' )
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==K_COUNT||(LA10_0>=K_CONSISTENCY && LA10_0<=K_LEVEL)||(LA10_0>=K_WRITETIME && LA10_0<=K_TTL)||(LA10_0>=K_VALUES && LA10_0<=K_TIMESTAMP)||(LA10_0>=K_KEY && LA10_0<=K_CLUSTERING)||LA10_0==IDENT||LA10_0==K_TYPE||LA10_0==QUOTED_NAME||(LA10_0>=K_ASCII && LA10_0<=K_TIMEUUID)) ) {
                alt10=1;
            }
            else if ( (LA10_0==116) ) {
                alt10=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:187:7: t1= selector ( ',' tN= selector )*
                    {
                    pushFollow(FOLLOW_selector_in_selectClause698);
                    t1=selector();

                    state._fsp--;

                     expr = new ArrayList<Selector>(); expr.add(t1); 
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:187:73: ( ',' tN= selector )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( (LA9_0==115) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:187:74: ',' tN= selector
                    	    {
                    	    match(input,115,FOLLOW_115_in_selectClause703); 
                    	    pushFollow(FOLLOW_selector_in_selectClause707);
                    	    tN=selector();

                    	    state._fsp--;

                    	     expr.add(tN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:188:7: '\\*'
                    {
                    match(input,116,FOLLOW_116_in_selectClause719); 
                     expr = Collections.<Selector>emptyList();

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectClause"


    // $ANTLR start "selector"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:191:1: selector returns [Selector s] : (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' );
    public final Selector selector() throws RecognitionException {
        Selector s = null;

        ColumnIdentifier c = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:192:5: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' )
            int alt11=3;
            switch ( input.LA(1) ) {
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case IDENT:
            case K_TYPE:
            case QUOTED_NAME:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt11=1;
                }
                break;
            case K_WRITETIME:
                {
                int LA11_2 = input.LA(2);

                if ( (LA11_2==113) ) {
                    alt11=2;
                }
                else if ( (LA11_2==K_FROM||LA11_2==115) ) {
                    alt11=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 11, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_TTL:
                {
                int LA11_3 = input.LA(2);

                if ( (LA11_3==113) ) {
                    alt11=3;
                }
                else if ( (LA11_3==K_FROM||LA11_3==115) ) {
                    alt11=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 11, 3, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }

            switch (alt11) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:192:7: c= cident
                    {
                    pushFollow(FOLLOW_cident_in_selector744);
                    c=cident();

                    state._fsp--;

                     s = c; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:193:7: K_WRITETIME '(' c= cident ')'
                    {
                    match(input,K_WRITETIME,FOLLOW_K_WRITETIME_in_selector766); 
                    match(input,113,FOLLOW_113_in_selector768); 
                    pushFollow(FOLLOW_cident_in_selector772);
                    c=cident();

                    state._fsp--;

                    match(input,114,FOLLOW_114_in_selector774); 
                     s = new Selector.WithFunction(c, Selector.Function.WRITE_TIME); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:194:7: K_TTL '(' c= cident ')'
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_selector784); 
                    match(input,113,FOLLOW_113_in_selector786); 
                    pushFollow(FOLLOW_cident_in_selector790);
                    c=cident();

                    state._fsp--;

                    match(input,114,FOLLOW_114_in_selector792); 
                     s = new Selector.WithFunction(c, Selector.Function.TTL); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return s;
    }
    // $ANTLR end "selector"


    // $ANTLR start "selectCountClause"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:197:1: selectCountClause returns [List<Selector> expr] : (ids= cidentList | '\\*' | i= INTEGER );
    public final List<Selector> selectCountClause() throws RecognitionException {
        List<Selector> expr = null;

        Token i=null;
        List<ColumnIdentifier> ids = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:198:5: (ids= cidentList | '\\*' | i= INTEGER )
            int alt12=3;
            switch ( input.LA(1) ) {
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case IDENT:
            case K_TYPE:
            case QUOTED_NAME:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt12=1;
                }
                break;
            case 116:
                {
                alt12=2;
                }
                break;
            case INTEGER:
                {
                alt12=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }

            switch (alt12) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:198:7: ids= cidentList
                    {
                    pushFollow(FOLLOW_cidentList_in_selectCountClause823);
                    ids=cidentList();

                    state._fsp--;

                     expr = new ArrayList<Selector>(ids); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:199:7: '\\*'
                    {
                    match(input,116,FOLLOW_116_in_selectCountClause833); 
                     expr = Collections.<Selector>emptyList();

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:200:7: i= INTEGER
                    {
                    i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectCountClause855); 
                     if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")"); expr = Collections.<Selector>emptyList();

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectCountClause"


    // $ANTLR start "whereClause"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:203:1: whereClause returns [List<Relation> clause] : first= relation ( K_AND next= relation )* ;
    public final List<Relation> whereClause() throws RecognitionException {
        List<Relation> clause = null;

        Relation first = null;

        Relation next = null;


         clause = new ArrayList<Relation>(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:205:5: (first= relation ( K_AND next= relation )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:205:7: first= relation ( K_AND next= relation )*
            {
            pushFollow(FOLLOW_relation_in_whereClause893);
            first=relation();

            state._fsp--;

             clause.add(first); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:205:46: ( K_AND next= relation )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==K_AND) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:205:47: K_AND next= relation
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_whereClause898); 
            	    pushFollow(FOLLOW_relation_in_whereClause902);
            	    next=relation();

            	    state._fsp--;

            	     clause.add(next); 

            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return clause;
    }
    // $ANTLR end "whereClause"


    // $ANTLR start "orderByClause"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:208:1: orderByClause[Map<ColumnIdentifier, Boolean> orderings] : c= cident ( K_ASC | K_DESC )? ;
    public final void orderByClause(Map<ColumnIdentifier, Boolean> orderings) throws RecognitionException {
        ColumnIdentifier c = null;



                ColumnIdentifier orderBy = null;
                boolean reversed = false;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:213:5: (c= cident ( K_ASC | K_DESC )? )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:213:7: c= cident ( K_ASC | K_DESC )?
            {
            pushFollow(FOLLOW_cident_in_orderByClause934);
            c=cident();

            state._fsp--;

             orderBy = c; 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:213:33: ( K_ASC | K_DESC )?
            int alt14=3;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==K_ASC) ) {
                alt14=1;
            }
            else if ( (LA14_0==K_DESC) ) {
                alt14=2;
            }
            switch (alt14) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:213:34: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_orderByClause939); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:213:42: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_orderByClause943); 
                     reversed = true; 

                    }
                    break;

            }

             orderings.put(c, reversed); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "orderByClause"


    // $ANTLR start "insertStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:216:1: insertStatement returns [UpdateStatement expr] : K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )+ ')' K_VALUES '(' v1= term ( ',' vn= term )+ ')' ( usingClause[attrs] )? ;
    public final UpdateStatement insertStatement() throws RecognitionException {
        UpdateStatement expr = null;

        CFName cf = null;

        ColumnIdentifier c1 = null;

        ColumnIdentifier cn = null;

        Term v1 = null;

        Term vn = null;



                Attributes attrs = new Attributes();
                List<ColumnIdentifier> columnNames  = new ArrayList<ColumnIdentifier>();
                List<Term> columnValues = new ArrayList<Term>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:229:5: ( K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )+ ')' K_VALUES '(' v1= term ( ',' vn= term )+ ')' ( usingClause[attrs] )? )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:229:7: K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )+ ')' K_VALUES '(' v1= term ( ',' vn= term )+ ')' ( usingClause[attrs] )?
            {
            match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement981); 
            match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement983); 
            pushFollow(FOLLOW_columnFamilyName_in_insertStatement987);
            cf=columnFamilyName();

            state._fsp--;

            match(input,113,FOLLOW_113_in_insertStatement999); 
            pushFollow(FOLLOW_cident_in_insertStatement1003);
            c1=cident();

            state._fsp--;

             columnNames.add(c1); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:230:51: ( ',' cn= cident )+
            int cnt15=0;
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==115) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:230:53: ',' cn= cident
            	    {
            	    match(input,115,FOLLOW_115_in_insertStatement1010); 
            	    pushFollow(FOLLOW_cident_in_insertStatement1014);
            	    cn=cident();

            	    state._fsp--;

            	     columnNames.add(cn); 

            	    }
            	    break;

            	default :
            	    if ( cnt15 >= 1 ) break loop15;
                        EarlyExitException eee =
                            new EarlyExitException(15, input);
                        throw eee;
                }
                cnt15++;
            } while (true);

            match(input,114,FOLLOW_114_in_insertStatement1021); 
            match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement1031); 
            match(input,113,FOLLOW_113_in_insertStatement1043); 
            pushFollow(FOLLOW_term_in_insertStatement1047);
            v1=term();

            state._fsp--;

             columnValues.add(v1); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:232:49: ( ',' vn= term )+
            int cnt16=0;
            loop16:
            do {
                int alt16=2;
                int LA16_0 = input.LA(1);

                if ( (LA16_0==115) ) {
                    alt16=1;
                }


                switch (alt16) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:232:51: ',' vn= term
            	    {
            	    match(input,115,FOLLOW_115_in_insertStatement1053); 
            	    pushFollow(FOLLOW_term_in_insertStatement1057);
            	    vn=term();

            	    state._fsp--;

            	     columnValues.add(vn); 

            	    }
            	    break;

            	default :
            	    if ( cnt16 >= 1 ) break loop16;
                        EarlyExitException eee =
                            new EarlyExitException(16, input);
                        throw eee;
                }
                cnt16++;
            } while (true);

            match(input,114,FOLLOW_114_in_insertStatement1064); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:233:9: ( usingClause[attrs] )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==K_USING) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:233:11: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_insertStatement1076);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }


                      expr = new UpdateStatement(cf, columnNames, columnValues, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "insertStatement"


    // $ANTLR start "usingClause"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:239:1: usingClause[Attributes attrs] : K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* ;
    public final void usingClause(Attributes attrs) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:5: ( K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:7: K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClause1106); 
            pushFollow(FOLLOW_usingClauseObjective_in_usingClause1108);
            usingClauseObjective(attrs);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:43: ( ( K_AND )? usingClauseObjective[attrs] )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==K_CONSISTENCY||(LA19_0>=K_TTL && LA19_0<=K_AND)||LA19_0==K_TIMESTAMP) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:45: ( K_AND )? usingClauseObjective[attrs]
            	    {
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:45: ( K_AND )?
            	    int alt18=2;
            	    int LA18_0 = input.LA(1);

            	    if ( (LA18_0==K_AND) ) {
            	        alt18=1;
            	    }
            	    switch (alt18) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:45: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClause1113); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseObjective_in_usingClause1116);
            	    usingClauseObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClause"


    // $ANTLR start "usingClauseDelete"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:243:1: usingClauseDelete[Attributes attrs] : K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* ;
    public final void usingClauseDelete(Attributes attrs) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:5: ( K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:7: K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete1138); 
            pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1140);
            usingClauseDeleteObjective(attrs);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:49: ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( (LA21_0==K_CONSISTENCY||LA21_0==K_AND||LA21_0==K_TIMESTAMP) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:51: ( K_AND )? usingClauseDeleteObjective[attrs]
            	    {
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:51: ( K_AND )?
            	    int alt20=2;
            	    int LA20_0 = input.LA(1);

            	    if ( (LA20_0==K_AND) ) {
            	        alt20=1;
            	    }
            	    switch (alt20) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:244:51: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClauseDelete1145); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1148);
            	    usingClauseDeleteObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop21;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseDelete"


    // $ANTLR start "usingClauseDeleteObjective"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:247:1: usingClauseDeleteObjective[Attributes attrs] : ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER );
    public final void usingClauseDeleteObjective(Attributes attrs) throws RecognitionException {
        Token ts=null;
        Token K_LEVEL2=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:248:5: ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER )
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==K_CONSISTENCY) ) {
                alt22=1;
            }
            else if ( (LA22_0==K_TIMESTAMP) ) {
                alt22=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 22, 0, input);

                throw nvae;
            }
            switch (alt22) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:248:7: K_CONSISTENCY K_LEVEL
                    {
                    match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective1170); 
                    K_LEVEL2=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_usingClauseDeleteObjective1172); 
                     attrs.cLevel = ConsistencyLevel.valueOf((K_LEVEL2!=null?K_LEVEL2.getText():null).toUpperCase()); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:249:7: K_TIMESTAMP ts= INTEGER
                    {
                    match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1183); 
                    ts=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseDeleteObjective1187); 
                     attrs.timestamp = Long.valueOf((ts!=null?ts.getText():null)); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseDeleteObjective"


    // $ANTLR start "usingClauseObjective"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:252:1: usingClauseObjective[Attributes attrs] : ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER );
    public final void usingClauseObjective(Attributes attrs) throws RecognitionException {
        Token t=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:253:5: ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER )
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==K_CONSISTENCY||LA23_0==K_TIMESTAMP) ) {
                alt23=1;
            }
            else if ( (LA23_0==K_TTL) ) {
                alt23=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 23, 0, input);

                throw nvae;
            }
            switch (alt23) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:253:7: usingClauseDeleteObjective[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1207);
                    usingClauseDeleteObjective(attrs);

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:254:7: K_TTL t= INTEGER
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective1216); 
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseObjective1220); 
                     attrs.timeToLive = Integer.valueOf((t!=null?t.getText():null)); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseObjective"


    // $ANTLR start "updateStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:257:1: updateStatement returns [UpdateStatement expr] : K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE wclause= whereClause ;
    public final UpdateStatement updateStatement() throws RecognitionException {
        UpdateStatement expr = null;

        CFName cf = null;

        List<Relation> wclause = null;



                Attributes attrs = new Attributes();
                Map<ColumnIdentifier, Operation> columns = new HashMap<ColumnIdentifier, Operation>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:268:5: ( K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE wclause= whereClause )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:268:7: K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE wclause= whereClause
            {
            match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1254); 
            pushFollow(FOLLOW_columnFamilyName_in_updateStatement1258);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:269:7: ( usingClause[attrs] )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==K_USING) ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:269:9: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_updateStatement1268);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_SET,FOLLOW_K_SET_in_updateStatement1280); 
            pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1282);
            termPairWithOperation(columns);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:270:44: ( ',' termPairWithOperation[columns] )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==115) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:270:45: ',' termPairWithOperation[columns]
            	    {
            	    match(input,115,FOLLOW_115_in_updateStatement1286); 
            	    pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1288);
            	    termPairWithOperation(columns);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);

            match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1299); 
            pushFollow(FOLLOW_whereClause_in_updateStatement1303);
            wclause=whereClause();

            state._fsp--;


                      return new UpdateStatement(cf, columns, wclause, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "updateStatement"


    // $ANTLR start "deleteStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:277:1: deleteStatement returns [DeleteStatement expr] : K_DELETE (ids= cidentList )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ;
    public final DeleteStatement deleteStatement() throws RecognitionException {
        DeleteStatement expr = null;

        List<ColumnIdentifier> ids = null;

        CFName cf = null;

        List<Relation> wclause = null;



                Attributes attrs = new Attributes();
                List<ColumnIdentifier> columnsList = Collections.emptyList();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:288:5: ( K_DELETE (ids= cidentList )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:288:7: K_DELETE (ids= cidentList )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause
            {
            match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1343); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:288:16: (ids= cidentList )?
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==K_COUNT||(LA26_0>=K_CONSISTENCY && LA26_0<=K_LEVEL)||(LA26_0>=K_WRITETIME && LA26_0<=K_TTL)||(LA26_0>=K_VALUES && LA26_0<=K_TIMESTAMP)||(LA26_0>=K_KEY && LA26_0<=K_CLUSTERING)||LA26_0==IDENT||LA26_0==K_TYPE||LA26_0==QUOTED_NAME||(LA26_0>=K_ASCII && LA26_0<=K_TIMEUUID)) ) {
                alt26=1;
            }
            switch (alt26) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:288:18: ids= cidentList
                    {
                    pushFollow(FOLLOW_cidentList_in_deleteStatement1349);
                    ids=cidentList();

                    state._fsp--;

                     columnsList = ids; 

                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1362); 
            pushFollow(FOLLOW_columnFamilyName_in_deleteStatement1366);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:290:7: ( usingClauseDelete[attrs] )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==K_USING) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:290:9: usingClauseDelete[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1376);
                    usingClauseDelete(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1388); 
            pushFollow(FOLLOW_whereClause_in_deleteStatement1392);
            wclause=whereClause();

            state._fsp--;


                      return new DeleteStatement(cf, columnsList, wclause, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "deleteStatement"


    // $ANTLR start "batchStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:298:1: batchStatement returns [BatchStatement expr] : K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH ;
    public final BatchStatement batchStatement() throws RecognitionException {
        BatchStatement expr = null;

        ModificationStatement s1 = null;

        ModificationStatement sN = null;



                Attributes attrs = new Attributes();
                List<ModificationStatement> statements = new ArrayList<ModificationStatement>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:327:5: ( K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:327:7: K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH
            {
            match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement1433); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1435); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:327:23: ( usingClause[attrs] )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==K_USING) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:327:25: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_batchStatement1439);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1457);
            s1=batchStatementObjective();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:38: ( ';' )?
            int alt29=2;
            int LA29_0 = input.LA(1);

            if ( (LA29_0==112) ) {
                alt29=1;
            }
            switch (alt29) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:38: ';'
                    {
                    match(input,112,FOLLOW_112_in_batchStatement1459); 

                    }
                    break;

            }

             statements.add(s1); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:67: (sN= batchStatementObjective ( ';' )? )*
            loop31:
            do {
                int alt31=2;
                int LA31_0 = input.LA(1);

                if ( (LA31_0==K_INSERT||LA31_0==K_UPDATE||LA31_0==K_DELETE) ) {
                    alt31=1;
                }


                switch (alt31) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:69: sN= batchStatementObjective ( ';' )?
            	    {
            	    pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1468);
            	    sN=batchStatementObjective();

            	    state._fsp--;

            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:96: ( ';' )?
            	    int alt30=2;
            	    int LA30_0 = input.LA(1);

            	    if ( (LA30_0==112) ) {
            	        alt30=1;
            	    }
            	    switch (alt30) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:328:96: ';'
            	            {
            	            match(input,112,FOLLOW_112_in_batchStatement1470); 

            	            }
            	            break;

            	    }

            	     statements.add(sN); 

            	    }
            	    break;

            	default :
            	    break loop31;
                }
            } while (true);

            match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement1484); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1486); 

                      return new BatchStatement(statements, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "batchStatement"


    // $ANTLR start "batchStatementObjective"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:335:1: batchStatementObjective returns [ModificationStatement statement] : (i= insertStatement | u= updateStatement | d= deleteStatement );
    public final ModificationStatement batchStatementObjective() throws RecognitionException {
        ModificationStatement statement = null;

        UpdateStatement i = null;

        UpdateStatement u = null;

        DeleteStatement d = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:336:5: (i= insertStatement | u= updateStatement | d= deleteStatement )
            int alt32=3;
            switch ( input.LA(1) ) {
            case K_INSERT:
                {
                alt32=1;
                }
                break;
            case K_UPDATE:
                {
                alt32=2;
                }
                break;
            case K_DELETE:
                {
                alt32=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }

            switch (alt32) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:336:7: i= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_batchStatementObjective1517);
                    i=insertStatement();

                    state._fsp--;

                     statement = i; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:337:7: u= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_batchStatementObjective1530);
                    u=updateStatement();

                    state._fsp--;

                     statement = u; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:338:7: d= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective1543);
                    d=deleteStatement();

                    state._fsp--;

                     statement = d; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return statement;
    }
    // $ANTLR end "batchStatementObjective"


    // $ANTLR start "createKeyspaceStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:341:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
    public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
        CreateKeyspaceStatement expr = null;

        String ks = null;


         KSPropDefs attrs = new KSPropDefs(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:346:5: ( K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:346:7: K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement1578); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1580); 
            pushFollow(FOLLOW_keyspaceName_in_createKeyspaceStatement1584);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement1592); 
            pushFollow(FOLLOW_properties_in_createKeyspaceStatement1594);
            properties(attrs);

            state._fsp--;

             expr = new CreateKeyspaceStatement(ks, attrs); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createKeyspaceStatement"


    // $ANTLR start "createColumnFamilyStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:350:1: createColumnFamilyStatement returns [CreateColumnFamilyStatement.RawStatement expr] : K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr] ;
    public final CreateColumnFamilyStatement.RawStatement createColumnFamilyStatement() throws RecognitionException {
        CreateColumnFamilyStatement.RawStatement expr = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:358:5: ( K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr] )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:358:7: K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createColumnFamilyStatement1620); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1622); 
            pushFollow(FOLLOW_columnFamilyName_in_createColumnFamilyStatement1626);
            cf=columnFamilyName();

            state._fsp--;

             expr = new CreateColumnFamilyStatement.RawStatement(cf); 
            pushFollow(FOLLOW_cfamDefinition_in_createColumnFamilyStatement1636);
            cfamDefinition(expr);

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createColumnFamilyStatement"


    // $ANTLR start "cfamDefinition"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:362:1: cfamDefinition[CreateColumnFamilyStatement.RawStatement expr] : '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? ;
    public final void cfamDefinition(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:5: ( '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:7: '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            {
            match(input,113,FOLLOW_113_in_cfamDefinition1655); 
            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition1657);
            cfamColumns(expr);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:29: ( ',' ( cfamColumns[expr] )? )*
            loop34:
            do {
                int alt34=2;
                int LA34_0 = input.LA(1);

                if ( (LA34_0==115) ) {
                    alt34=1;
                }


                switch (alt34) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:31: ',' ( cfamColumns[expr] )?
            	    {
            	    match(input,115,FOLLOW_115_in_cfamDefinition1662); 
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:35: ( cfamColumns[expr] )?
            	    int alt33=2;
            	    int LA33_0 = input.LA(1);

            	    if ( (LA33_0==K_COUNT||(LA33_0>=K_CONSISTENCY && LA33_0<=K_LEVEL)||(LA33_0>=K_WRITETIME && LA33_0<=K_TTL)||(LA33_0>=K_VALUES && LA33_0<=K_TIMESTAMP)||(LA33_0>=K_PRIMARY && LA33_0<=K_CLUSTERING)||LA33_0==IDENT||LA33_0==K_TYPE||LA33_0==QUOTED_NAME||(LA33_0>=K_ASCII && LA33_0<=K_TIMEUUID)) ) {
            	        alt33=1;
            	    }
            	    switch (alt33) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:363:35: cfamColumns[expr]
            	            {
            	            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition1664);
            	            cfamColumns(expr);

            	            state._fsp--;


            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    break loop34;
                }
            } while (true);

            match(input,114,FOLLOW_114_in_cfamDefinition1671); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:364:7: ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==K_WITH) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:364:9: K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_cfamDefinition1681); 
                    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition1683);
                    cfamProperty(expr);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:364:35: ( K_AND cfamProperty[expr] )*
                    loop35:
                    do {
                        int alt35=2;
                        int LA35_0 = input.LA(1);

                        if ( (LA35_0==K_AND) ) {
                            alt35=1;
                        }


                        switch (alt35) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:364:37: K_AND cfamProperty[expr]
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_cfamDefinition1688); 
                    	    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition1690);
                    	    cfamProperty(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop35;
                        }
                    } while (true);


                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamDefinition"


    // $ANTLR start "cfamColumns"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:367:1: cfamColumns[CreateColumnFamilyStatement.RawStatement expr] : (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' k= cident ( ',' c= cident )* ')' );
    public final void cfamColumns(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        String v = null;

        ColumnIdentifier c = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:368:5: (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' k= cident ( ',' c= cident )* ')' )
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==K_COUNT||(LA39_0>=K_CONSISTENCY && LA39_0<=K_LEVEL)||(LA39_0>=K_WRITETIME && LA39_0<=K_TTL)||(LA39_0>=K_VALUES && LA39_0<=K_TIMESTAMP)||(LA39_0>=K_KEY && LA39_0<=K_CLUSTERING)||LA39_0==IDENT||LA39_0==K_TYPE||LA39_0==QUOTED_NAME||(LA39_0>=K_ASCII && LA39_0<=K_TIMEUUID)) ) {
                alt39=1;
            }
            else if ( (LA39_0==K_PRIMARY) ) {
                alt39=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 39, 0, input);

                throw nvae;
            }
            switch (alt39) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:368:7: k= cident v= comparatorType ( K_PRIMARY K_KEY )?
                    {
                    pushFollow(FOLLOW_cident_in_cfamColumns1716);
                    k=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_cfamColumns1720);
                    v=comparatorType();

                    state._fsp--;

                     expr.addDefinition(k, v); 
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:368:64: ( K_PRIMARY K_KEY )?
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==K_PRIMARY) ) {
                        alt37=1;
                    }
                    switch (alt37) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:368:65: K_PRIMARY K_KEY
                            {
                            match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns1725); 
                            match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns1727); 
                             expr.setKeyAlias(k); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:369:7: K_PRIMARY K_KEY '(' k= cident ( ',' c= cident )* ')'
                    {
                    match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns1739); 
                    match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns1741); 
                    match(input,113,FOLLOW_113_in_cfamColumns1743); 
                    pushFollow(FOLLOW_cident_in_cfamColumns1747);
                    k=cident();

                    state._fsp--;

                     expr.setKeyAlias(k); 
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:369:62: ( ',' c= cident )*
                    loop38:
                    do {
                        int alt38=2;
                        int LA38_0 = input.LA(1);

                        if ( (LA38_0==115) ) {
                            alt38=1;
                        }


                        switch (alt38) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:369:63: ',' c= cident
                    	    {
                    	    match(input,115,FOLLOW_115_in_cfamColumns1752); 
                    	    pushFollow(FOLLOW_cident_in_cfamColumns1756);
                    	    c=cident();

                    	    state._fsp--;

                    	     expr.addColumnAlias(c); 

                    	    }
                    	    break;

                    	default :
                    	    break loop38;
                        }
                    } while (true);

                    match(input,114,FOLLOW_114_in_cfamColumns1763); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamColumns"


    // $ANTLR start "cfamProperty"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:372:1: cfamProperty[CreateColumnFamilyStatement.RawStatement expr] : (k= propertyKey '=' v= propertyValue | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' );
    public final void cfamProperty(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        String k = null;

        String v = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:373:5: (k= propertyKey '=' v= propertyValue | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' )
            int alt41=3;
            switch ( input.LA(1) ) {
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_KEY:
            case K_STORAGE:
            case IDENT:
            case K_TYPE:
            case QUOTED_NAME:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt41=1;
                }
                break;
            case K_COMPACT:
                {
                int LA41_2 = input.LA(2);

                if ( (LA41_2==K_STORAGE) ) {
                    alt41=2;
                }
                else if ( (LA41_2==117||LA41_2==121) ) {
                    alt41=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 41, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_CLUSTERING:
                {
                int LA41_3 = input.LA(2);

                if ( (LA41_3==K_ORDER) ) {
                    alt41=3;
                }
                else if ( (LA41_3==117||LA41_3==121) ) {
                    alt41=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 41, 3, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 41, 0, input);

                throw nvae;
            }

            switch (alt41) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:373:7: k= propertyKey '=' v= propertyValue
                    {
                    pushFollow(FOLLOW_propertyKey_in_cfamProperty1783);
                    k=propertyKey();

                    state._fsp--;

                    match(input,117,FOLLOW_117_in_cfamProperty1785); 
                    pushFollow(FOLLOW_propertyValue_in_cfamProperty1789);
                    v=propertyValue();

                    state._fsp--;

                     try { expr.addProperty(k, v); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:374:7: K_COMPACT K_STORAGE
                    {
                    match(input,K_COMPACT,FOLLOW_K_COMPACT_in_cfamProperty1799); 
                    match(input,K_STORAGE,FOLLOW_K_STORAGE_in_cfamProperty1801); 
                     expr.setCompactStorage(); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:375:7: K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')'
                    {
                    match(input,K_CLUSTERING,FOLLOW_K_CLUSTERING_in_cfamProperty1811); 
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_cfamProperty1813); 
                    match(input,K_BY,FOLLOW_K_BY_in_cfamProperty1815); 
                    match(input,113,FOLLOW_113_in_cfamProperty1817); 
                    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty1819);
                    cfamOrdering(expr);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:375:56: ( ',' cfamOrdering[expr] )*
                    loop40:
                    do {
                        int alt40=2;
                        int LA40_0 = input.LA(1);

                        if ( (LA40_0==115) ) {
                            alt40=1;
                        }


                        switch (alt40) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:375:57: ',' cfamOrdering[expr]
                    	    {
                    	    match(input,115,FOLLOW_115_in_cfamProperty1823); 
                    	    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty1825);
                    	    cfamOrdering(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop40;
                        }
                    } while (true);

                    match(input,114,FOLLOW_114_in_cfamProperty1830); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamProperty"


    // $ANTLR start "cfamOrdering"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:378:1: cfamOrdering[CreateColumnFamilyStatement.RawStatement expr] : k= cident ( K_ASC | K_DESC ) ;
    public final void cfamOrdering(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;


         boolean reversed=false; 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:380:5: (k= cident ( K_ASC | K_DESC ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:380:7: k= cident ( K_ASC | K_DESC )
            {
            pushFollow(FOLLOW_cident_in_cfamOrdering1858);
            k=cident();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:380:16: ( K_ASC | K_DESC )
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==K_ASC) ) {
                alt42=1;
            }
            else if ( (LA42_0==K_DESC) ) {
                alt42=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 42, 0, input);

                throw nvae;
            }
            switch (alt42) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:380:17: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_cfamOrdering1861); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:380:25: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_cfamOrdering1865); 
                     reversed=true;

                    }
                    break;

            }

             expr.setOrdering(k, reversed); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfamOrdering"


    // $ANTLR start "createIndexStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:383:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' ;
    public final CreateIndexStatement createIndexStatement() throws RecognitionException {
        CreateIndexStatement expr = null;

        Token idxName=null;
        CFName cf = null;

        ColumnIdentifier id = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:387:5: ( K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:387:7: K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')'
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement1894); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement1896); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:387:24: (idxName= IDENT )?
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==IDENT) ) {
                alt43=1;
            }
            switch (alt43) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:387:25: idxName= IDENT
                    {
                    idxName=(Token)match(input,IDENT,FOLLOW_IDENT_in_createIndexStatement1901); 

                    }
                    break;

            }

            match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement1905); 
            pushFollow(FOLLOW_columnFamilyName_in_createIndexStatement1909);
            cf=columnFamilyName();

            state._fsp--;

            match(input,113,FOLLOW_113_in_createIndexStatement1911); 
            pushFollow(FOLLOW_cident_in_createIndexStatement1915);
            id=cident();

            state._fsp--;

            match(input,114,FOLLOW_114_in_createIndexStatement1917); 
             expr = new CreateIndexStatement(cf, (idxName!=null?idxName.getText():null), id); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createIndexStatement"


    // $ANTLR start "alterKeyspaceStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:391:1: alterKeyspaceStatement returns [AlterKeyspaceStatement expr] : K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
    public final AlterKeyspaceStatement alterKeyspaceStatement() throws RecognitionException {
        AlterKeyspaceStatement expr = null;

        String ks = null;


         KSPropDefs attrs = new KSPropDefs(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:396:5: ( K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:396:7: K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterKeyspaceStatement1957); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement1959); 
            pushFollow(FOLLOW_keyspaceName_in_alterKeyspaceStatement1963);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_alterKeyspaceStatement1973); 
            pushFollow(FOLLOW_properties_in_alterKeyspaceStatement1975);
            properties(attrs);

            state._fsp--;

             expr = new AlterKeyspaceStatement(ks, attrs); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterKeyspaceStatement"


    // $ANTLR start "alterTableStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:401:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] ) ;
    public final AlterTableStatement alterTableStatement() throws RecognitionException {
        AlterTableStatement expr = null;

        CFName cf = null;

        ColumnIdentifier id = null;

        String v = null;



                AlterTableStatement.Type type = null;
                CFPropDefs props = new CFPropDefs();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:412:5: ( K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:412:7: K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] )
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2011); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2013); 
            pushFollow(FOLLOW_columnFamilyName_in_alterTableStatement2017);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:413:11: ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_DROP id= cident | K_WITH properties[props] )
            int alt44=4;
            switch ( input.LA(1) ) {
            case K_ALTER:
                {
                alt44=1;
                }
                break;
            case K_ADD:
                {
                alt44=2;
                }
                break;
            case K_DROP:
                {
                alt44=3;
                }
                break;
            case K_WITH:
                {
                alt44=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }

            switch (alt44) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:413:13: K_ALTER id= cident K_TYPE v= comparatorType
                    {
                    match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2031); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2035);
                    id=cident();

                    state._fsp--;

                    match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement2037); 
                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2041);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ALTER; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:414:13: K_ADD id= cident v= comparatorType
                    {
                    match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement2057); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2063);
                    id=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2067);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ADD; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:415:13: K_DROP id= cident
                    {
                    match(input,K_DROP,FOLLOW_K_DROP_in_alterTableStatement2090); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2095);
                    id=cident();

                    state._fsp--;

                     type = AlterTableStatement.Type.DROP; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:416:13: K_WITH properties[props]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_alterTableStatement2135); 
                    pushFollow(FOLLOW_properties_in_alterTableStatement2138);
                    properties(props);

                    state._fsp--;

                     type = AlterTableStatement.Type.OPTS; 

                    }
                    break;

            }


                    expr = new AlterTableStatement(cf, type, id, v, props);
                

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterTableStatement"


    // $ANTLR start "dropKeyspaceStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:423:1: dropKeyspaceStatement returns [DropKeyspaceStatement ksp] : K_DROP K_KEYSPACE ks= keyspaceName ;
    public final DropKeyspaceStatement dropKeyspaceStatement() throws RecognitionException {
        DropKeyspaceStatement ksp = null;

        String ks = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:427:5: ( K_DROP K_KEYSPACE ks= keyspaceName )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:427:7: K_DROP K_KEYSPACE ks= keyspaceName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement2198); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2200); 
            pushFollow(FOLLOW_keyspaceName_in_dropKeyspaceStatement2204);
            ks=keyspaceName();

            state._fsp--;

             ksp = new DropKeyspaceStatement(ks); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ksp;
    }
    // $ANTLR end "dropKeyspaceStatement"


    // $ANTLR start "dropColumnFamilyStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:430:1: dropColumnFamilyStatement returns [DropColumnFamilyStatement stmt] : K_DROP K_COLUMNFAMILY cf= columnFamilyName ;
    public final DropColumnFamilyStatement dropColumnFamilyStatement() throws RecognitionException {
        DropColumnFamilyStatement stmt = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:434:5: ( K_DROP K_COLUMNFAMILY cf= columnFamilyName )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:434:7: K_DROP K_COLUMNFAMILY cf= columnFamilyName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropColumnFamilyStatement2229); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2231); 
            pushFollow(FOLLOW_columnFamilyName_in_dropColumnFamilyStatement2235);
            cf=columnFamilyName();

            state._fsp--;

             stmt = new DropColumnFamilyStatement(cf); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "dropColumnFamilyStatement"


    // $ANTLR start "dropIndexStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:437:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX index= IDENT ;
    public final DropIndexStatement dropIndexStatement() throws RecognitionException {
        DropIndexStatement expr = null;

        Token index=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:441:5: ( K_DROP K_INDEX index= IDENT )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:442:7: K_DROP K_INDEX index= IDENT
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement2266); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement2268); 
            index=(Token)match(input,IDENT,FOLLOW_IDENT_in_dropIndexStatement2272); 
             expr = new DropIndexStatement((index!=null?index.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "dropIndexStatement"


    // $ANTLR start "truncateStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:446:1: truncateStatement returns [TruncateStatement stmt] : K_TRUNCATE cf= columnFamilyName ;
    public final TruncateStatement truncateStatement() throws RecognitionException {
        TruncateStatement stmt = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:5: ( K_TRUNCATE cf= columnFamilyName )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:7: K_TRUNCATE cf= columnFamilyName
            {
            match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement2303); 
            pushFollow(FOLLOW_columnFamilyName_in_truncateStatement2307);
            cf=columnFamilyName();

            state._fsp--;

             stmt = new TruncateStatement(cf); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "truncateStatement"


    // $ANTLR start "grantStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:453:1: grantStatement returns [GrantStatement stmt] : K_GRANT permission K_ON resource= columnFamilyName K_TO user= ( IDENT | STRING_LITERAL ) ( K_WITH K_GRANT K_OPTION )? ;
    public final GrantStatement grantStatement() throws RecognitionException {
        GrantStatement stmt = null;

        Token user=null;
        CFName resource = null;

        Permission permission3 = null;


         boolean withGrant = false; 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:458:5: ( K_GRANT permission K_ON resource= columnFamilyName K_TO user= ( IDENT | STRING_LITERAL ) ( K_WITH K_GRANT K_OPTION )? )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:458:7: K_GRANT permission K_ON resource= columnFamilyName K_TO user= ( IDENT | STRING_LITERAL ) ( K_WITH K_GRANT K_OPTION )?
            {
            match(input,K_GRANT,FOLLOW_K_GRANT_in_grantStatement2341); 
            pushFollow(FOLLOW_permission_in_grantStatement2353);
            permission3=permission();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_grantStatement2361); 
            pushFollow(FOLLOW_columnFamilyName_in_grantStatement2375);
            resource=columnFamilyName();

            state._fsp--;

            match(input,K_TO,FOLLOW_K_TO_in_grantStatement2383); 
            user=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:464:7: ( K_WITH K_GRANT K_OPTION )?
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==K_WITH) ) {
                alt45=1;
            }
            switch (alt45) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:464:8: K_WITH K_GRANT K_OPTION
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_grantStatement2412); 
                    match(input,K_GRANT,FOLLOW_K_GRANT_in_grantStatement2414); 
                    match(input,K_OPTION,FOLLOW_K_OPTION_in_grantStatement2416); 
                     withGrant = true; 

                    }
                    break;

            }


                    stmt = new GrantStatement(permission3,
                                               resource,
                                               (user!=null?user.getText():null),
                                               withGrant);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "grantStatement"


    // $ANTLR start "revokeStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:473:1: revokeStatement returns [RevokeStatement stmt] : K_REVOKE permission K_ON resource= columnFamilyName K_FROM user= ( IDENT | STRING_LITERAL ) ;
    public final RevokeStatement revokeStatement() throws RecognitionException {
        RevokeStatement stmt = null;

        Token user=null;
        CFName resource = null;

        Permission permission4 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:477:5: ( K_REVOKE permission K_ON resource= columnFamilyName K_FROM user= ( IDENT | STRING_LITERAL ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:477:7: K_REVOKE permission K_ON resource= columnFamilyName K_FROM user= ( IDENT | STRING_LITERAL )
            {
            match(input,K_REVOKE,FOLLOW_K_REVOKE_in_revokeStatement2451); 
            pushFollow(FOLLOW_permission_in_revokeStatement2461);
            permission4=permission();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_revokeStatement2469); 
            pushFollow(FOLLOW_columnFamilyName_in_revokeStatement2481);
            resource=columnFamilyName();

            state._fsp--;

            match(input,K_FROM,FOLLOW_K_FROM_in_revokeStatement2489); 
            user=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


                    stmt = new RevokeStatement(permission4,
                                                (user!=null?user.getText():null),
                                                resource);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "revokeStatement"


    // $ANTLR start "listGrantsStatement"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:490:1: listGrantsStatement returns [ListGrantsStatement stmt] : K_LIST K_GRANTS K_FOR username= ( IDENT | STRING_LITERAL ) ;
    public final ListGrantsStatement listGrantsStatement() throws RecognitionException {
        ListGrantsStatement stmt = null;

        Token username=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:491:5: ( K_LIST K_GRANTS K_FOR username= ( IDENT | STRING_LITERAL ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:491:7: K_LIST K_GRANTS K_FOR username= ( IDENT | STRING_LITERAL )
            {
            match(input,K_LIST,FOLLOW_K_LIST_in_listGrantsStatement2536); 
            match(input,K_GRANTS,FOLLOW_K_GRANTS_in_listGrantsStatement2538); 
            match(input,K_FOR,FOLLOW_K_FOR_in_listGrantsStatement2540); 
            username=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             stmt = new ListGrantsStatement((username!=null?username.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmt;
    }
    // $ANTLR end "listGrantsStatement"


    // $ANTLR start "permission"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:494:1: permission returns [Permission perm] : p= ( K_DESCRIBE | K_USE | K_CREATE | K_ALTER | K_DROP | K_SELECT | K_INSERT | K_UPDATE | K_DELETE | K_FULL_ACCESS | K_NO_ACCESS ) ;
    public final Permission permission() throws RecognitionException {
        Permission perm = null;

        Token p=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:495:5: (p= ( K_DESCRIBE | K_USE | K_CREATE | K_ALTER | K_DROP | K_SELECT | K_INSERT | K_UPDATE | K_DELETE | K_FULL_ACCESS | K_NO_ACCESS ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:495:7: p= ( K_DESCRIBE | K_USE | K_CREATE | K_ALTER | K_DROP | K_SELECT | K_INSERT | K_UPDATE | K_DELETE | K_FULL_ACCESS | K_NO_ACCESS )
            {
            p=(Token)input.LT(1);
            if ( (input.LA(1)>=K_USE && input.LA(1)<=K_SELECT)||input.LA(1)==K_INSERT||input.LA(1)==K_UPDATE||input.LA(1)==K_DELETE||input.LA(1)==K_CREATE||input.LA(1)==K_ALTER||input.LA(1)==K_DROP||(input.LA(1)>=K_DESCRIBE && input.LA(1)<=K_NO_ACCESS) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             perm = Permission.valueOf((p!=null?p.getText():null).toUpperCase()); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return perm;
    }
    // $ANTLR end "permission"


    // $ANTLR start "cident"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:498:1: cident returns [ColumnIdentifier id] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final ColumnIdentifier cident() throws RecognitionException {
        ColumnIdentifier id = null;

        Token t=null;
        String k = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:502:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt46=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt46=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt46=2;
                }
                break;
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt46=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 46, 0, input);

                throw nvae;
            }

            switch (alt46) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:502:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cident2649); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:503:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cident2674); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:504:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cident2693);
                    k=unreserved_keyword();

                    state._fsp--;

                     id = new ColumnIdentifier(k, false); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return id;
    }
    // $ANTLR end "cident"


    // $ANTLR start "keyspaceName"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:508:1: keyspaceName returns [String id] : cfOrKsName[name, true] ;
    public final String keyspaceName() throws RecognitionException {
        String id = null;

         CFName name = new CFName(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:510:5: ( cfOrKsName[name, true] )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:510:7: cfOrKsName[name, true]
            {
            pushFollow(FOLLOW_cfOrKsName_in_keyspaceName2726);
            cfOrKsName(name, true);

            state._fsp--;

             id = name.getKeyspace(); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return id;
    }
    // $ANTLR end "keyspaceName"


    // $ANTLR start "columnFamilyName"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:513:1: columnFamilyName returns [CFName name] : ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] ;
    public final CFName columnFamilyName() throws RecognitionException {
        CFName name = null;

         name = new CFName(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:515:5: ( ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:515:7: ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false]
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:515:7: ( cfOrKsName[name, true] '.' )?
            int alt47=2;
            switch ( input.LA(1) ) {
                case IDENT:
                    {
                    int LA47_1 = input.LA(2);

                    if ( (LA47_1==118) ) {
                        alt47=1;
                    }
                    }
                    break;
                case QUOTED_NAME:
                    {
                    int LA47_2 = input.LA(2);

                    if ( (LA47_2==118) ) {
                        alt47=1;
                    }
                    }
                    break;
                case K_COUNT:
                case K_CONSISTENCY:
                case K_LEVEL:
                case K_WRITETIME:
                case K_TTL:
                case K_VALUES:
                case K_KEY:
                case K_COMPACT:
                case K_STORAGE:
                case K_CLUSTERING:
                case K_TYPE:
                    {
                    int LA47_3 = input.LA(2);

                    if ( (LA47_3==118) ) {
                        alt47=1;
                    }
                    }
                    break;
                case K_TIMESTAMP:
                case K_ASCII:
                case K_BIGINT:
                case K_BLOB:
                case K_BOOLEAN:
                case K_COUNTER:
                case K_DECIMAL:
                case K_DOUBLE:
                case K_FLOAT:
                case K_INT:
                case K_TEXT:
                case K_UUID:
                case K_VARCHAR:
                case K_VARINT:
                case K_TIMEUUID:
                    {
                    int LA47_4 = input.LA(2);

                    if ( (LA47_4==118) ) {
                        alt47=1;
                    }
                    }
                    break;
            }

            switch (alt47) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:515:8: cfOrKsName[name, true] '.'
                    {
                    pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName2760);
                    cfOrKsName(name, true);

                    state._fsp--;

                    match(input,118,FOLLOW_118_in_columnFamilyName2763); 

                    }
                    break;

            }

            pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName2767);
            cfOrKsName(name, false);

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return name;
    }
    // $ANTLR end "columnFamilyName"


    // $ANTLR start "cfOrKsName"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:518:1: cfOrKsName[CFName name, boolean isKs] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final void cfOrKsName(CFName name, boolean isKs) throws RecognitionException {
        Token t=null;
        String k = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:519:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt48=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt48=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt48=2;
                }
                break;
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt48=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 48, 0, input);

                throw nvae;
            }

            switch (alt48) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:519:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cfOrKsName2788); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), false); else name.setColumnFamily((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:520:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cfOrKsName2813); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), true); else name.setColumnFamily((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:521:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cfOrKsName2832);
                    k=unreserved_keyword();

                    state._fsp--;

                     if (isKs) name.setKeyspace(k, false); else name.setColumnFamily(k, false); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "cfOrKsName"


    // $ANTLR start "cidentList"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:524:1: cidentList returns [List<ColumnIdentifier> items] : t1= cident ( ',' tN= cident )* ;
    public final List<ColumnIdentifier> cidentList() throws RecognitionException {
        List<ColumnIdentifier> items = null;

        ColumnIdentifier t1 = null;

        ColumnIdentifier tN = null;


         items = new ArrayList<ColumnIdentifier>(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:526:5: (t1= cident ( ',' tN= cident )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:526:8: t1= cident ( ',' tN= cident )*
            {
            pushFollow(FOLLOW_cident_in_cidentList2866);
            t1=cident();

            state._fsp--;

             items.add(t1); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:526:38: ( ',' tN= cident )*
            loop49:
            do {
                int alt49=2;
                int LA49_0 = input.LA(1);

                if ( (LA49_0==115) ) {
                    alt49=1;
                }


                switch (alt49) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:526:39: ',' tN= cident
            	    {
            	    match(input,115,FOLLOW_115_in_cidentList2871); 
            	    pushFollow(FOLLOW_cident_in_cidentList2875);
            	    tN=cident();

            	    state._fsp--;

            	     items.add(tN); 

            	    }
            	    break;

            	default :
            	    break loop49;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return items;
    }
    // $ANTLR end "cidentList"


    // $ANTLR start "extendedTerm"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:530:1: extendedTerm returns [Term term] : ( K_TOKEN '(' t= term ')' | t= term );
    public final Term extendedTerm() throws RecognitionException {
        Term term = null;

        Term t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:531:5: ( K_TOKEN '(' t= term ')' | t= term )
            int alt50=2;
            int LA50_0 = input.LA(1);

            if ( (LA50_0==K_TOKEN) ) {
                alt50=1;
            }
            else if ( (LA50_0==INTEGER||LA50_0==STRING_LITERAL||(LA50_0>=UUID && LA50_0<=QMARK)) ) {
                alt50=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 50, 0, input);

                throw nvae;
            }
            switch (alt50) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:531:7: K_TOKEN '(' t= term ')'
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_extendedTerm2901); 
                    match(input,113,FOLLOW_113_in_extendedTerm2903); 
                    pushFollow(FOLLOW_term_in_extendedTerm2907);
                    t=term();

                    state._fsp--;

                    match(input,114,FOLLOW_114_in_extendedTerm2909); 
                     term = Term.tokenOf(t); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:532:7: t= term
                    {
                    pushFollow(FOLLOW_term_in_extendedTerm2921);
                    t=term();

                    state._fsp--;

                     term = t; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return term;
    }
    // $ANTLR end "extendedTerm"


    // $ANTLR start "term"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:535:1: term returns [Term term] : (t= ( STRING_LITERAL | UUID | INTEGER | FLOAT ) | t= QMARK );
    public final Term term() throws RecognitionException {
        Term term = null;

        Token t=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:536:5: (t= ( STRING_LITERAL | UUID | INTEGER | FLOAT ) | t= QMARK )
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==INTEGER||LA51_0==STRING_LITERAL||(LA51_0>=UUID && LA51_0<=FLOAT)) ) {
                alt51=1;
            }
            else if ( (LA51_0==QMARK) ) {
                alt51=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 51, 0, input);

                throw nvae;
            }
            switch (alt51) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:536:7: t= ( STRING_LITERAL | UUID | INTEGER | FLOAT )
                    {
                    t=(Token)input.LT(1);
                    if ( input.LA(1)==INTEGER||input.LA(1)==STRING_LITERAL||(input.LA(1)>=UUID && input.LA(1)<=FLOAT) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     term = new Term((t!=null?t.getText():null), (t!=null?t.getType():0)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:537:7: t= QMARK
                    {
                    t=(Token)match(input,QMARK,FOLLOW_QMARK_in_term2989); 
                     term = new Term((t!=null?t.getText():null), (t!=null?t.getType():0), ++currentBindMarkerIdx); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return term;
    }
    // $ANTLR end "term"


    // $ANTLR start "intTerm"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:540:1: intTerm returns [Term integer] : (t= INTEGER | t= QMARK );
    public final Term intTerm() throws RecognitionException {
        Term integer = null;

        Token t=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:541:5: (t= INTEGER | t= QMARK )
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==INTEGER) ) {
                alt52=1;
            }
            else if ( (LA52_0==QMARK) ) {
                alt52=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 52, 0, input);

                throw nvae;
            }
            switch (alt52) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:541:7: t= INTEGER
                    {
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_intTerm3051); 
                     integer = new Term((t!=null?t.getText():null), (t!=null?t.getType():0)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:542:7: t= QMARK
                    {
                    t=(Token)match(input,QMARK,FOLLOW_QMARK_in_intTerm3063); 
                     integer = new Term((t!=null?t.getText():null), (t!=null?t.getType():0), ++currentBindMarkerIdx); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return integer;
    }
    // $ANTLR end "intTerm"


    // $ANTLR start "termPairWithOperation"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:545:1: termPairWithOperation[Map<ColumnIdentifier, Operation> columns] : key= cident '=' (value= term | c= cident ( '+' v= intTerm | (op= '-' )? v= intTerm ) ) ;
    public final void termPairWithOperation(Map<ColumnIdentifier, Operation> columns) throws RecognitionException {
        Token op=null;
        ColumnIdentifier key = null;

        Term value = null;

        ColumnIdentifier c = null;

        Term v = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:546:5: (key= cident '=' (value= term | c= cident ( '+' v= intTerm | (op= '-' )? v= intTerm ) ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:546:7: key= cident '=' (value= term | c= cident ( '+' v= intTerm | (op= '-' )? v= intTerm ) )
            {
            pushFollow(FOLLOW_cident_in_termPairWithOperation3087);
            key=cident();

            state._fsp--;

            match(input,117,FOLLOW_117_in_termPairWithOperation3089); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:547:9: (value= term | c= cident ( '+' v= intTerm | (op= '-' )? v= intTerm ) )
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==INTEGER||LA55_0==STRING_LITERAL||(LA55_0>=UUID && LA55_0<=QMARK)) ) {
                alt55=1;
            }
            else if ( (LA55_0==K_COUNT||(LA55_0>=K_CONSISTENCY && LA55_0<=K_LEVEL)||(LA55_0>=K_WRITETIME && LA55_0<=K_TTL)||(LA55_0>=K_VALUES && LA55_0<=K_TIMESTAMP)||(LA55_0>=K_KEY && LA55_0<=K_CLUSTERING)||LA55_0==IDENT||LA55_0==K_TYPE||LA55_0==QUOTED_NAME||(LA55_0>=K_ASCII && LA55_0<=K_TIMEUUID)) ) {
                alt55=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 55, 0, input);

                throw nvae;
            }
            switch (alt55) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:547:11: value= term
                    {
                    pushFollow(FOLLOW_term_in_termPairWithOperation3103);
                    value=term();

                    state._fsp--;

                     columns.put(key, new Operation(value)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:548:11: c= cident ( '+' v= intTerm | (op= '-' )? v= intTerm )
                    {
                    pushFollow(FOLLOW_cident_in_termPairWithOperation3119);
                    c=cident();

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:548:20: ( '+' v= intTerm | (op= '-' )? v= intTerm )
                    int alt54=2;
                    int LA54_0 = input.LA(1);

                    if ( (LA54_0==119) ) {
                        alt54=1;
                    }
                    else if ( (LA54_0==INTEGER||LA54_0==QMARK||LA54_0==120) ) {
                        alt54=2;
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 54, 0, input);

                        throw nvae;
                    }
                    switch (alt54) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:548:22: '+' v= intTerm
                            {
                            match(input,119,FOLLOW_119_in_termPairWithOperation3123); 
                            pushFollow(FOLLOW_intTerm_in_termPairWithOperation3131);
                            v=intTerm();

                            state._fsp--;

                             columns.put(key, new Operation(c, Operation.Type.PLUS, v)); 

                            }
                            break;
                        case 2 :
                            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:549:22: (op= '-' )? v= intTerm
                            {
                            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:549:24: (op= '-' )?
                            int alt53=2;
                            int LA53_0 = input.LA(1);

                            if ( (LA53_0==120) ) {
                                alt53=1;
                            }
                            switch (alt53) {
                                case 1 :
                                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:549:24: op= '-'
                                    {
                                    op=(Token)match(input,120,FOLLOW_120_in_termPairWithOperation3158); 

                                    }
                                    break;

                            }

                            pushFollow(FOLLOW_intTerm_in_termPairWithOperation3163);
                            v=intTerm();

                            state._fsp--;


                                                   validateMinusSupplied(op, v, input);
                                                   if (op == null)
                                                       v = new Term(-(Long.valueOf(v.getText())), v.getType());
                                                   columns.put(key, new Operation(c, Operation.Type.MINUS, v));
                                                 

                            }
                            break;

                    }


                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "termPairWithOperation"


    // $ANTLR start "properties"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:560:1: properties[PropertyDefinitions props] : property[props] ( K_AND property[props] )* ;
    public final void properties(PropertyDefinitions props) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:561:5: ( property[props] ( K_AND property[props] )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:561:7: property[props] ( K_AND property[props] )*
            {
            pushFollow(FOLLOW_property_in_properties3236);
            property(props);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:561:23: ( K_AND property[props] )*
            loop56:
            do {
                int alt56=2;
                int LA56_0 = input.LA(1);

                if ( (LA56_0==K_AND) ) {
                    alt56=1;
                }


                switch (alt56) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:561:24: K_AND property[props]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_properties3240); 
            	    pushFollow(FOLLOW_property_in_properties3242);
            	    property(props);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop56;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "properties"


    // $ANTLR start "property"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:564:1: property[PropertyDefinitions props] : k= propertyKey '=' simple= propertyValue ;
    public final void property(PropertyDefinitions props) throws RecognitionException {
        String k = null;

        String simple = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:565:5: (k= propertyKey '=' simple= propertyValue )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:565:7: k= propertyKey '=' simple= propertyValue
            {
            pushFollow(FOLLOW_propertyKey_in_property3265);
            k=propertyKey();

            state._fsp--;

            match(input,117,FOLLOW_117_in_property3267); 
            pushFollow(FOLLOW_propertyValue_in_property3271);
            simple=propertyValue();

            state._fsp--;

             try { props.addProperty(k, simple); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "property"


    // $ANTLR start "propertyKey"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:568:1: propertyKey returns [String str] : c1= cident ( ':' cn= cident )* ;
    public final String propertyKey() throws RecognitionException {
        String str = null;

        ColumnIdentifier c1 = null;

        ColumnIdentifier cn = null;


         StringBuilder sb = new StringBuilder(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:570:5: (c1= cident ( ':' cn= cident )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:570:7: c1= cident ( ':' cn= cident )*
            {
            pushFollow(FOLLOW_cident_in_propertyKey3304);
            c1=cident();

            state._fsp--;

             sb.append(c1); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:570:36: ( ':' cn= cident )*
            loop57:
            do {
                int alt57=2;
                int LA57_0 = input.LA(1);

                if ( (LA57_0==121) ) {
                    alt57=1;
                }


                switch (alt57) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:570:38: ':' cn= cident
            	    {
            	    match(input,121,FOLLOW_121_in_propertyKey3310); 
            	    pushFollow(FOLLOW_cident_in_propertyKey3314);
            	    cn=cident();

            	    state._fsp--;

            	     sb.append(':').append(cn); 

            	    }
            	    break;

            	default :
            	    break loop57;
                }
            } while (true);

             str = sb.toString(); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "propertyKey"


    // $ANTLR start "propertyValue"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:573:1: propertyValue returns [String str] : (v= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) | u= unreserved_keyword );
    public final String propertyValue() throws RecognitionException {
        String str = null;

        Token v=null;
        String u = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:5: (v= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) | u= unreserved_keyword )
            int alt58=2;
            int LA58_0 = input.LA(1);

            if ( (LA58_0==INTEGER||LA58_0==IDENT||LA58_0==STRING_LITERAL||LA58_0==FLOAT) ) {
                alt58=1;
            }
            else if ( (LA58_0==K_COUNT||(LA58_0>=K_CONSISTENCY && LA58_0<=K_LEVEL)||(LA58_0>=K_WRITETIME && LA58_0<=K_TTL)||(LA58_0>=K_VALUES && LA58_0<=K_TIMESTAMP)||(LA58_0>=K_KEY && LA58_0<=K_CLUSTERING)||LA58_0==K_TYPE||(LA58_0>=K_ASCII && LA58_0<=K_TIMEUUID)) ) {
                alt58=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 58, 0, input);

                throw nvae;
            }
            switch (alt58) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:7: v= ( STRING_LITERAL | IDENT | INTEGER | FLOAT )
                    {
                    v=(Token)input.LT(1);
                    if ( input.LA(1)==INTEGER||input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL||input.LA(1)==FLOAT ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     str = (v!=null?v.getText():null); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:575:7: u= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_propertyValue3370);
                    u=unreserved_keyword();

                    state._fsp--;

                     str = u; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "propertyValue"


    // $ANTLR start "relation"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:578:1: relation returns [Relation rel] : (name= cident type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term | K_TOKEN '(' name= cident ')' type= ( '=' | '<' | '<=' | '>=' | '>' ) t= extendedTerm | name= cident K_IN '(' f1= term ( ',' fN= term )* ')' );
    public final Relation relation() throws RecognitionException {
        Relation rel = null;

        Token type=null;
        ColumnIdentifier name = null;

        Term t = null;

        Term f1 = null;

        Term fN = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:579:5: (name= cident type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term | K_TOKEN '(' name= cident ')' type= ( '=' | '<' | '<=' | '>=' | '>' ) t= extendedTerm | name= cident K_IN '(' f1= term ( ',' fN= term )* ')' )
            int alt60=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                int LA60_1 = input.LA(2);

                if ( (LA60_1==117||(LA60_1>=122 && LA60_1<=125)) ) {
                    alt60=1;
                }
                else if ( (LA60_1==K_IN) ) {
                    alt60=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 60, 1, input);

                    throw nvae;
                }
                }
                break;
            case QUOTED_NAME:
                {
                int LA60_2 = input.LA(2);

                if ( (LA60_2==K_IN) ) {
                    alt60=3;
                }
                else if ( (LA60_2==117||(LA60_2>=122 && LA60_2<=125)) ) {
                    alt60=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 60, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_COUNT:
            case K_CONSISTENCY:
            case K_LEVEL:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
                {
                int LA60_3 = input.LA(2);

                if ( (LA60_3==K_IN) ) {
                    alt60=3;
                }
                else if ( (LA60_3==117||(LA60_3>=122 && LA60_3<=125)) ) {
                    alt60=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 60, 3, input);

                    throw nvae;
                }
                }
                break;
            case K_TIMESTAMP:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_COUNTER:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                int LA60_4 = input.LA(2);

                if ( (LA60_4==117||(LA60_4>=122 && LA60_4<=125)) ) {
                    alt60=1;
                }
                else if ( (LA60_4==K_IN) ) {
                    alt60=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 60, 4, input);

                    throw nvae;
                }
                }
                break;
            case K_TOKEN:
                {
                alt60=2;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 60, 0, input);

                throw nvae;
            }

            switch (alt60) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:579:7: name= cident type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term
                    {
                    pushFollow(FOLLOW_cident_in_relation3419);
                    name=cident();

                    state._fsp--;

                    type=(Token)input.LT(1);
                    if ( input.LA(1)==117||(input.LA(1)>=122 && input.LA(1)<=125) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                    pushFollow(FOLLOW_term_in_relation3445);
                    t=term();

                    state._fsp--;

                     rel = new Relation(name, (type!=null?type.getText():null), t); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:580:7: K_TOKEN '(' name= cident ')' type= ( '=' | '<' | '<=' | '>=' | '>' ) t= extendedTerm
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_relation3455); 
                    match(input,113,FOLLOW_113_in_relation3457); 
                    pushFollow(FOLLOW_cident_in_relation3461);
                    name=cident();

                    state._fsp--;

                    match(input,114,FOLLOW_114_in_relation3463); 
                    type=(Token)input.LT(1);
                    if ( input.LA(1)==117||(input.LA(1)>=122 && input.LA(1)<=125) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                    pushFollow(FOLLOW_extendedTerm_in_relation3488);
                    t=extendedTerm();

                    state._fsp--;

                     rel = new Relation(name, (type!=null?type.getText():null), t, true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:581:7: name= cident K_IN '(' f1= term ( ',' fN= term )* ')'
                    {
                    pushFollow(FOLLOW_cident_in_relation3500);
                    name=cident();

                    state._fsp--;

                    match(input,K_IN,FOLLOW_K_IN_in_relation3502); 
                     rel = Relation.createInRelation(name); 
                    match(input,113,FOLLOW_113_in_relation3512); 
                    pushFollow(FOLLOW_term_in_relation3516);
                    f1=term();

                    state._fsp--;

                     rel.addInValue(f1); 
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:582:44: ( ',' fN= term )*
                    loop59:
                    do {
                        int alt59=2;
                        int LA59_0 = input.LA(1);

                        if ( (LA59_0==115) ) {
                            alt59=1;
                        }


                        switch (alt59) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:582:45: ',' fN= term
                    	    {
                    	    match(input,115,FOLLOW_115_in_relation3521); 
                    	    pushFollow(FOLLOW_term_in_relation3525);
                    	    fN=term();

                    	    state._fsp--;

                    	     rel.addInValue(fN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop59;
                        }
                    } while (true);

                    match(input,114,FOLLOW_114_in_relation3532); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return rel;
    }
    // $ANTLR end "relation"


    // $ANTLR start "comparatorType"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:585:1: comparatorType returns [String str] : (c= native_type | s= STRING_LITERAL );
    public final String comparatorType() throws RecognitionException {
        String str = null;

        Token s=null;
        String c = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:586:5: (c= native_type | s= STRING_LITERAL )
            int alt61=2;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==K_TIMESTAMP||(LA61_0>=K_ASCII && LA61_0<=K_TIMEUUID)) ) {
                alt61=1;
            }
            else if ( (LA61_0==STRING_LITERAL) ) {
                alt61=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 61, 0, input);

                throw nvae;
            }
            switch (alt61) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:586:7: c= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_comparatorType3555);
                    c=native_type();

                    state._fsp--;

                     str =c; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:587:7: s= STRING_LITERAL
                    {
                    s=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_comparatorType3570); 
                     str = (s!=null?s.getText():null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "comparatorType"


    // $ANTLR start "native_type"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:590:1: native_type returns [String str] : c= ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID ) ;
    public final String native_type() throws RecognitionException {
        String str = null;

        Token c=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:591:5: (c= ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:591:7: c= ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID )
            {
            c=(Token)input.LT(1);
            if ( input.LA(1)==K_TIMESTAMP||(input.LA(1)>=K_ASCII && input.LA(1)<=K_TIMEUUID) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             return (c!=null?c.getText():null); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "native_type"


    // $ANTLR start "unreserved_keyword"
    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:609:1: unreserved_keyword returns [String str] : (k= ( K_KEY | K_CONSISTENCY | K_CLUSTERING | K_LEVEL | K_COUNT | K_TTL | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_WRITETIME ) | t= native_type );
    public final String unreserved_keyword() throws RecognitionException {
        String str = null;

        Token k=null;
        String t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:610:5: (k= ( K_KEY | K_CONSISTENCY | K_CLUSTERING | K_LEVEL | K_COUNT | K_TTL | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_WRITETIME ) | t= native_type )
            int alt62=2;
            int LA62_0 = input.LA(1);

            if ( (LA62_0==K_COUNT||(LA62_0>=K_CONSISTENCY && LA62_0<=K_LEVEL)||(LA62_0>=K_WRITETIME && LA62_0<=K_TTL)||LA62_0==K_VALUES||(LA62_0>=K_KEY && LA62_0<=K_CLUSTERING)||LA62_0==K_TYPE) ) {
                alt62=1;
            }
            else if ( (LA62_0==K_TIMESTAMP||(LA62_0>=K_ASCII && LA62_0<=K_TIMEUUID)) ) {
                alt62=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 62, 0, input);

                throw nvae;
            }
            switch (alt62) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:610:7: k= ( K_KEY | K_CONSISTENCY | K_CLUSTERING | K_LEVEL | K_COUNT | K_TTL | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_WRITETIME )
                    {
                    k=(Token)input.LT(1);
                    if ( input.LA(1)==K_COUNT||(input.LA(1)>=K_CONSISTENCY && input.LA(1)<=K_LEVEL)||(input.LA(1)>=K_WRITETIME && input.LA(1)<=K_TTL)||input.LA(1)==K_VALUES||(input.LA(1)>=K_KEY && input.LA(1)<=K_CLUSTERING)||input.LA(1)==K_TYPE ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     str = (k!=null?k.getText():null); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:622:7: t= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_unreserved_keyword3942);
                    t=native_type();

                    state._fsp--;

                     str = t; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "unreserved_keyword"

    // Delegated rules


    protected DFA2 dfa2 = new DFA2(this);
    static final String DFA2_eotS =
        "\26\uffff";
    static final String DFA2_eofS =
        "\26\uffff";
    static final String DFA2_minS =
        "\1\4\7\uffff\3\40\13\uffff";
    static final String DFA2_maxS =
        "\1\65\7\uffff\2\50\1\42\13\uffff";
    static final String DFA2_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\3\uffff\1\17\1\20\1\21\1\10"+
        "\1\11\1\12\1\13\1\14\1\15\1\16\1\22";
    static final String DFA2_specialS =
        "\26\uffff}>";
    static final String[] DFA2_transitionS = {
            "\1\6\1\1\17\uffff\1\2\3\uffff\1\3\1\uffff\1\5\1\4\2\uffff\1"+
            "\10\13\uffff\1\12\2\uffff\1\11\1\7\1\13\3\uffff\1\14\1\15",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\16\1\uffff\1\17\5\uffff\1\20",
            "\1\21\1\uffff\1\22\5\uffff\1\23",
            "\1\25\1\uffff\1\24",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA2_eot = DFA.unpackEncodedString(DFA2_eotS);
    static final short[] DFA2_eof = DFA.unpackEncodedString(DFA2_eofS);
    static final char[] DFA2_min = DFA.unpackEncodedStringToUnsignedChars(DFA2_minS);
    static final char[] DFA2_max = DFA.unpackEncodedStringToUnsignedChars(DFA2_maxS);
    static final short[] DFA2_accept = DFA.unpackEncodedString(DFA2_acceptS);
    static final short[] DFA2_special = DFA.unpackEncodedString(DFA2_specialS);
    static final short[][] DFA2_transition;

    static {
        int numStates = DFA2_transitionS.length;
        DFA2_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA2_transition[i] = DFA.unpackEncodedString(DFA2_transitionS[i]);
        }
    }

    class DFA2 extends DFA {

        public DFA2(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 2;
            this.eot = DFA2_eot;
            this.eof = DFA2_eof;
            this.min = DFA2_min;
            this.max = DFA2_max;
            this.accept = DFA2_accept;
            this.special = DFA2_special;
            this.transition = DFA2_transition;
        }
        public String getDescription() {
            return "128:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= grantStatement | st16= revokeStatement | st17= listGrantsStatement | st18= alterKeyspaceStatement );";
        }
    }
 

    public static final BitSet FOLLOW_cqlStatement_in_query72 = new BitSet(new long[]{0x0000000000000000L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_query75 = new BitSet(new long[]{0x0000000000000000L,0x0001000000000000L});
    public static final BitSet FOLLOW_EOF_in_query79 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectStatement_in_cqlStatement113 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_cqlStatement138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_cqlStatement163 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_batchStatement_in_cqlStatement188 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_cqlStatement214 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useStatement_in_cqlStatement239 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_truncateStatement_in_cqlStatement267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createKeyspaceStatement_in_cqlStatement290 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createColumnFamilyStatement_in_cqlStatement307 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createIndexStatement_in_cqlStatement319 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropKeyspaceStatement_in_cqlStatement338 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropColumnFamilyStatement_in_cqlStatement356 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropIndexStatement_in_cqlStatement370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterTableStatement_in_cqlStatement391 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantStatement_in_cqlStatement411 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeStatement_in_cqlStatement436 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listGrantsStatement_in_cqlStatement460 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterKeyspaceStatement_in_cqlStatement480 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USE_in_useStatement510 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_keyspaceName_in_useStatement514 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SELECT_in_selectStatement548 = new BitSet(new long[]{0x080012F001830640L,0x0010000000007FFEL});
    public static final BitSet FOLLOW_selectClause_in_selectStatement554 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_COUNT_in_selectStatement559 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_selectStatement561 = new BitSet(new long[]{0x080012F001838640L,0x0010000000007FFEL});
    public static final BitSet FOLLOW_selectCountClause_in_selectStatement565 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_selectStatement567 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_selectStatement580 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_selectStatement584 = new BitSet(new long[]{0x0000000000005902L});
    public static final BitSet FOLLOW_K_USING_in_selectStatement594 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_CONSISTENCY_in_selectStatement596 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_K_LEVEL_in_selectStatement598 = new BitSet(new long[]{0x0000000000005802L});
    public static final BitSet FOLLOW_K_WHERE_in_selectStatement613 = new BitSet(new long[]{0x180012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_whereClause_in_selectStatement617 = new BitSet(new long[]{0x0000000000005002L});
    public static final BitSet FOLLOW_K_ORDER_in_selectStatement630 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_K_BY_in_selectStatement632 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement634 = new BitSet(new long[]{0x0000000000004002L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_selectStatement639 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement641 = new BitSet(new long[]{0x0000000000004002L,0x0008000000000000L});
    public static final BitSet FOLLOW_K_LIMIT_in_selectStatement658 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_INTEGER_in_selectStatement662 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selector_in_selectClause698 = new BitSet(new long[]{0x0000000000000002L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_selectClause703 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_selector_in_selectClause707 = new BitSet(new long[]{0x0000000000000002L,0x0008000000000000L});
    public static final BitSet FOLLOW_116_in_selectClause719 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_selector744 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WRITETIME_in_selector766 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_selector768 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_selector772 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_selector774 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_selector784 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_selector786 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_selector790 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_selector792 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cidentList_in_selectCountClause823 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_116_in_selectCountClause833 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_selectCountClause855 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_relation_in_whereClause893 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_K_AND_in_whereClause898 = new BitSet(new long[]{0x180012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_relation_in_whereClause902 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_cident_in_orderByClause934 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_K_ASC_in_orderByClause939 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_orderByClause943 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INSERT_in_insertStatement981 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_K_INTO_in_insertStatement983 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_insertStatement987 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_insertStatement999 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_insertStatement1003 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_insertStatement1010 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_insertStatement1014 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_insertStatement1021 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_K_VALUES_in_insertStatement1031 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_insertStatement1043 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_insertStatement1047 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_insertStatement1053 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_insertStatement1057 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_insertStatement1064 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_usingClause_in_insertStatement1076 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USING_in_usingClause1106 = new BitSet(new long[]{0x0000000001020200L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1108 = new BitSet(new long[]{0x0000000001060202L});
    public static final BitSet FOLLOW_K_AND_in_usingClause1113 = new BitSet(new long[]{0x0000000001020200L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1116 = new BitSet(new long[]{0x0000000001060202L});
    public static final BitSet FOLLOW_K_USING_in_usingClauseDelete1138 = new BitSet(new long[]{0x0000000001000200L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1140 = new BitSet(new long[]{0x0000000001040202L});
    public static final BitSet FOLLOW_K_AND_in_usingClauseDelete1145 = new BitSet(new long[]{0x0000000001000200L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1148 = new BitSet(new long[]{0x0000000001040202L});
    public static final BitSet FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective1170 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_K_LEVEL_in_usingClauseDeleteObjective1172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1183 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseDeleteObjective1187 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1207 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective1216 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseObjective1220 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1254 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_updateStatement1258 = new BitSet(new long[]{0x0000000004000100L});
    public static final BitSet FOLLOW_usingClause_in_updateStatement1268 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_SET_in_updateStatement1280 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1282 = new BitSet(new long[]{0x0000000000000800L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_updateStatement1286 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1288 = new BitSet(new long[]{0x0000000000000800L,0x0008000000000000L});
    public static final BitSet FOLLOW_K_WHERE_in_updateStatement1299 = new BitSet(new long[]{0x180012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_whereClause_in_updateStatement1303 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1343 = new BitSet(new long[]{0x080012F0018306C0L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cidentList_in_deleteStatement1349 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_deleteStatement1362 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_deleteStatement1366 = new BitSet(new long[]{0x0000000000000900L});
    public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1376 = new BitSet(new long[]{0x0000000000000800L});
    public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1388 = new BitSet(new long[]{0x180012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_whereClause_in_deleteStatement1392 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BEGIN_in_batchStatement1433 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1435 = new BitSet(new long[]{0x000000000A200100L});
    public static final BitSet FOLLOW_usingClause_in_batchStatement1439 = new BitSet(new long[]{0x000000000A200100L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1457 = new BitSet(new long[]{0x000000004A200100L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_batchStatement1459 = new BitSet(new long[]{0x000000004A200100L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1468 = new BitSet(new long[]{0x000000004A200100L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_batchStatement1470 = new BitSet(new long[]{0x000000004A200100L});
    public static final BitSet FOLLOW_K_APPLY_in_batchStatement1484 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1486 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective1517 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective1530 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective1543 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement1578 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1580 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_keyspaceName_in_createKeyspaceStatement1584 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement1592 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_properties_in_createKeyspaceStatement1594 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createColumnFamilyStatement1620 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1622 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_createColumnFamilyStatement1626 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_cfamDefinition_in_createColumnFamilyStatement1636 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_113_in_cfamDefinition1655 = new BitSet(new long[]{0x080012F801830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition1657 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_115_in_cfamDefinition1662 = new BitSet(new long[]{0x080012F801830640L,0x000C000000007FFEL});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition1664 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_cfamDefinition1671 = new BitSet(new long[]{0x0000000200000002L});
    public static final BitSet FOLLOW_K_WITH_in_cfamDefinition1681 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition1683 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_K_AND_in_cfamDefinition1688 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition1690 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_cident_in_cfamColumns1716 = new BitSet(new long[]{0x080412F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_comparatorType_in_cfamColumns1720 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns1725 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns1727 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns1739 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns1741 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_cfamColumns1743 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_cfamColumns1747 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_115_in_cfamColumns1752 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_cfamColumns1756 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_cfamColumns1763 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_propertyKey_in_cfamProperty1783 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L});
    public static final BitSet FOLLOW_117_in_cfamProperty1785 = new BitSet(new long[]{0x480412F001838640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_propertyValue_in_cfamProperty1789 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COMPACT_in_cfamProperty1799 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_K_STORAGE_in_cfamProperty1801 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CLUSTERING_in_cfamProperty1811 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_K_ORDER_in_cfamProperty1813 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_K_BY_in_cfamProperty1815 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_cfamProperty1817 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty1819 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_115_in_cfamProperty1823 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty1825 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_cfamProperty1830 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_cfamOrdering1858 = new BitSet(new long[]{0x0000000000180000L});
    public static final BitSet FOLLOW_K_ASC_in_cfamOrdering1861 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_cfamOrdering1865 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement1894 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement1896 = new BitSet(new long[]{0x0000060000000000L});
    public static final BitSet FOLLOW_IDENT_in_createIndexStatement1901 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_K_ON_in_createIndexStatement1905 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_createIndexStatement1909 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_createIndexStatement1911 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_createIndexStatement1915 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_createIndexStatement1917 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterKeyspaceStatement1957 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement1959 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_keyspaceName_in_alterKeyspaceStatement1963 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_K_WITH_in_alterKeyspaceStatement1973 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_properties_in_alterKeyspaceStatement1975 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2011 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2013 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_alterTableStatement2017 = new BitSet(new long[]{0x0000680200000000L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2031 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2035 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement2037 = new BitSet(new long[]{0x080412F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2041 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ADD_in_alterTableStatement2057 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2063 = new BitSet(new long[]{0x080412F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2067 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_alterTableStatement2090 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2095 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WITH_in_alterTableStatement2135 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_properties_in_alterTableStatement2138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement2198 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2200 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_keyspaceName_in_dropKeyspaceStatement2204 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropColumnFamilyStatement2229 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2231 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_dropColumnFamilyStatement2235 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement2266 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement2268 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_IDENT_in_dropIndexStatement2272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement2303 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_truncateStatement2307 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_GRANT_in_grantStatement2341 = new BitSet(new long[]{0x070048008A200030L});
    public static final BitSet FOLLOW_permission_in_grantStatement2353 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_K_ON_in_grantStatement2361 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_grantStatement2375 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_TO_in_grantStatement2383 = new BitSet(new long[]{0x0004020000000000L});
    public static final BitSet FOLLOW_set_in_grantStatement2397 = new BitSet(new long[]{0x0000000200000002L});
    public static final BitSet FOLLOW_K_WITH_in_grantStatement2412 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_K_GRANT_in_grantStatement2414 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_K_OPTION_in_grantStatement2416 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_REVOKE_in_revokeStatement2451 = new BitSet(new long[]{0x070048008A200030L});
    public static final BitSet FOLLOW_permission_in_revokeStatement2461 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_K_ON_in_revokeStatement2469 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_columnFamilyName_in_revokeStatement2481 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_revokeStatement2489 = new BitSet(new long[]{0x0004020000000000L});
    public static final BitSet FOLLOW_set_in_revokeStatement2501 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_listGrantsStatement2536 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_K_GRANTS_in_listGrantsStatement2538 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_K_FOR_in_listGrantsStatement2540 = new BitSet(new long[]{0x0004020000000000L});
    public static final BitSet FOLLOW_set_in_listGrantsStatement2544 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_permission2575 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cident2649 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cident2674 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cident2693 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_keyspaceName2726 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName2760 = new BitSet(new long[]{0x0000000000000000L,0x0040000000000000L});
    public static final BitSet FOLLOW_118_in_columnFamilyName2763 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName2767 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cfOrKsName2788 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cfOrKsName2813 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cfOrKsName2832 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_cidentList2866 = new BitSet(new long[]{0x0000000000000002L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_cidentList2871 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_cidentList2875 = new BitSet(new long[]{0x0000000000000002L,0x0008000000000000L});
    public static final BitSet FOLLOW_K_TOKEN_in_extendedTerm2901 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_extendedTerm2903 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_extendedTerm2907 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_extendedTerm2909 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_extendedTerm2921 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_term2962 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QMARK_in_term2989 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_intTerm3051 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QMARK_in_intTerm3063 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_termPairWithOperation3087 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L});
    public static final BitSet FOLLOW_117_in_termPairWithOperation3089 = new BitSet(new long[]{0xE80412F001838640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_term_in_termPairWithOperation3103 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_termPairWithOperation3119 = new BitSet(new long[]{0x8000000000008000L,0x0180000000000000L});
    public static final BitSet FOLLOW_119_in_termPairWithOperation3123 = new BitSet(new long[]{0x8000000000008000L,0x0180000000000000L});
    public static final BitSet FOLLOW_intTerm_in_termPairWithOperation3131 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_120_in_termPairWithOperation3158 = new BitSet(new long[]{0x8000000000008000L,0x0180000000000000L});
    public static final BitSet FOLLOW_intTerm_in_termPairWithOperation3163 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_property_in_properties3236 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_K_AND_in_properties3240 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_property_in_properties3242 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_propertyKey_in_property3265 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L});
    public static final BitSet FOLLOW_117_in_property3267 = new BitSet(new long[]{0x480412F001838640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_propertyValue_in_property3271 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_propertyKey3304 = new BitSet(new long[]{0x0000000000000002L,0x0200000000000000L});
    public static final BitSet FOLLOW_121_in_propertyKey3310 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_propertyKey3314 = new BitSet(new long[]{0x0000000000000002L,0x0200000000000000L});
    public static final BitSet FOLLOW_set_in_propertyValue3344 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_propertyValue3370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation3419 = new BitSet(new long[]{0x0000000000000000L,0x3C20000000000000L});
    public static final BitSet FOLLOW_set_in_relation3423 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_relation3445 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TOKEN_in_relation3455 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_relation3457 = new BitSet(new long[]{0x080012F001830640L,0x0000000000007FFEL});
    public static final BitSet FOLLOW_cident_in_relation3461 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_relation3463 = new BitSet(new long[]{0x0000000000000000L,0x3C20000000000000L});
    public static final BitSet FOLLOW_set_in_relation3467 = new BitSet(new long[]{0xF004000000008000L});
    public static final BitSet FOLLOW_extendedTerm_in_relation3488 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation3500 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_IN_in_relation3502 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_relation3512 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_relation3516 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_115_in_relation3521 = new BitSet(new long[]{0xE004000000008000L});
    public static final BitSet FOLLOW_term_in_relation3525 = new BitSet(new long[]{0x0000000000000000L,0x000C000000000000L});
    public static final BitSet FOLLOW_114_in_relation3532 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_comparatorType3555 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_comparatorType3570 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_native_type3595 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unreserved_keyword3798 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_unreserved_keyword3942 = new BitSet(new long[]{0x0000000000000002L});

}