// $ANTLR 3.2 Sep 23, 2009 12:02:23 /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g 2013-02-21 02:44:17

    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.auth.DataResource;
    import org.apache.cassandra.auth.IResource;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.functions.FunctionCall;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "K_USE", "K_SELECT", "K_COUNT", "K_FROM", "K_WHERE", "K_ORDER", "K_BY", "K_LIMIT", "INTEGER", "K_ALLOW", "K_FILTERING", "K_WRITETIME", "K_TTL", "K_AND", "K_ASC", "K_DESC", "K_INSERT", "K_INTO", "K_VALUES", "K_USING", "K_TIMESTAMP", "K_UPDATE", "K_SET", "K_DELETE", "K_BEGIN", "K_UNLOGGED", "K_COUNTER", "K_BATCH", "K_APPLY", "K_CREATE", "K_KEYSPACE", "K_WITH", "K_COLUMNFAMILY", "K_PRIMARY", "K_KEY", "K_COMPACT", "K_STORAGE", "K_CLUSTERING", "K_INDEX", "IDENT", "K_ON", "K_ALTER", "K_TYPE", "K_ADD", "K_RENAME", "K_TO", "K_DROP", "K_TRUNCATE", "K_GRANT", "K_REVOKE", "K_LIST", "K_OF", "K_NORECURSIVE", "K_MODIFY", "K_AUTHORIZE", "K_ALL", "K_PERMISSIONS", "K_PERMISSION", "K_KEYSPACES", "K_USER", "K_SUPERUSER", "K_NOSUPERUSER", "K_USERS", "K_PASSWORD", "STRING_LITERAL", "QUOTED_NAME", "FLOAT", "BOOLEAN", "UUID", "HEXNUMBER", "QMARK", "K_TOKEN", "K_IN", "K_ASCII", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_DECIMAL", "K_DOUBLE", "K_FLOAT", "K_INET", "K_INT", "K_TEXT", "K_UUID", "K_VARCHAR", "K_VARINT", "K_TIMEUUID", "K_MAP", "S", "E", "L", "C", "T", "F", "R", "O", "M", "W", "H", "A", "N", "D", "K", "Y", "I", "U", "P", "G", "B", "X", "V", "Z", "J", "Q", "DIGIT", "LETTER", "HEX", "EXPONENT", "WS", "COMMENT", "MULTILINE_COMMENT", "';'", "'('", "')'", "','", "'\\*'", "'['", "']'", "'.'", "'}'", "':'", "'{'", "'='", "'+'", "'-'", "'<'", "'<='", "'>'", "'>='"
    };
    public static final int EXPONENT=121;
    public static final int K_PERMISSIONS=60;
    public static final int LETTER=119;
    public static final int K_INT=85;
    public static final int K_PERMISSION=61;
    public static final int K_CREATE=33;
    public static final int K_CLUSTERING=41;
    public static final int K_WRITETIME=15;
    public static final int EOF=-1;
    public static final int K_PRIMARY=37;
    public static final int K_AUTHORIZE=58;
    public static final int K_VALUES=22;
    public static final int K_USE=4;
    public static final int STRING_LITERAL=68;
    public static final int K_GRANT=52;
    public static final int K_ON=44;
    public static final int K_USING=23;
    public static final int K_ADD=47;
    public static final int K_ASC=18;
    public static final int K_KEY=38;
    public static final int COMMENT=123;
    public static final int K_TRUNCATE=51;
    public static final int K_ORDER=9;
    public static final int K_OF=55;
    public static final int K_ALL=59;
    public static final int HEXNUMBER=73;
    public static final int D=105;
    public static final int T__139=139;
    public static final int E=93;
    public static final int T__138=138;
    public static final int F=97;
    public static final int T__137=137;
    public static final int G=111;
    public static final int T__136=136;
    public static final int K_COUNT=6;
    public static final int K_KEYSPACE=34;
    public static final int K_TYPE=46;
    public static final int A=103;
    public static final int B=112;
    public static final int C=95;
    public static final int L=94;
    public static final int M=100;
    public static final int N=104;
    public static final int O=99;
    public static final int H=102;
    public static final int I=108;
    public static final int J=116;
    public static final int K_UPDATE=25;
    public static final int K=106;
    public static final int K_FILTERING=14;
    public static final int U=109;
    public static final int T=96;
    public static final int W=101;
    public static final int K_TEXT=86;
    public static final int V=114;
    public static final int Q=117;
    public static final int P=110;
    public static final int K_COMPACT=39;
    public static final int S=92;
    public static final int R=98;
    public static final int T__141=141;
    public static final int T__142=142;
    public static final int K_TTL=16;
    public static final int T__140=140;
    public static final int Y=107;
    public static final int X=113;
    public static final int Z=115;
    public static final int T__126=126;
    public static final int T__125=125;
    public static final int K_INDEX=42;
    public static final int T__128=128;
    public static final int K_INSERT=20;
    public static final int T__127=127;
    public static final int WS=122;
    public static final int T__129=129;
    public static final int K_RENAME=48;
    public static final int K_APPLY=32;
    public static final int K_INET=84;
    public static final int K_STORAGE=40;
    public static final int K_TIMESTAMP=24;
    public static final int K_AND=17;
    public static final int K_DESC=19;
    public static final int T__130=130;
    public static final int K_TOKEN=75;
    public static final int QMARK=74;
    public static final int T__131=131;
    public static final int T__132=132;
    public static final int T__133=133;
    public static final int T__134=134;
    public static final int K_UUID=87;
    public static final int T__135=135;
    public static final int K_BATCH=31;
    public static final int K_ASCII=77;
    public static final int UUID=72;
    public static final int K_LIST=54;
    public static final int K_DELETE=27;
    public static final int K_TO=49;
    public static final int K_BY=10;
    public static final int FLOAT=70;
    public static final int K_VARINT=89;
    public static final int K_FLOAT=83;
    public static final int K_SUPERUSER=64;
    public static final int K_DOUBLE=82;
    public static final int K_SELECT=5;
    public static final int K_LIMIT=11;
    public static final int K_BOOLEAN=80;
    public static final int K_ALTER=45;
    public static final int K_SET=26;
    public static final int K_WHERE=8;
    public static final int QUOTED_NAME=69;
    public static final int MULTILINE_COMMENT=124;
    public static final int K_BLOB=79;
    public static final int K_UNLOGGED=29;
    public static final int BOOLEAN=71;
    public static final int HEX=120;
    public static final int K_INTO=21;
    public static final int K_PASSWORD=67;
    public static final int K_REVOKE=53;
    public static final int K_ALLOW=13;
    public static final int K_VARCHAR=88;
    public static final int IDENT=43;
    public static final int DIGIT=118;
    public static final int K_USERS=66;
    public static final int K_BEGIN=28;
    public static final int INTEGER=12;
    public static final int K_KEYSPACES=62;
    public static final int K_COUNTER=30;
    public static final int K_DECIMAL=81;
    public static final int K_WITH=35;
    public static final int K_IN=76;
    public static final int K_NORECURSIVE=56;
    public static final int K_MAP=91;
    public static final int K_FROM=7;
    public static final int K_COLUMNFAMILY=36;
    public static final int K_MODIFY=57;
    public static final int K_DROP=50;
    public static final int K_NOSUPERUSER=65;
    public static final int K_BIGINT=78;
    public static final int K_TIMEUUID=90;
    public static final int K_USER=63;

    // delegates
    // delegators


        public CqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return CqlParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g"; }


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

        public void throwLastRecognitionError() throws SyntaxException
        {
            if (recognitionErrors.size() > 0)
                throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
        }

        public Map<String, String> convertPropertyMap(Maps.Literal map)
        {
            if (map == null || map.entries == null || map.entries.isEmpty())
                return Collections.<String, String>emptyMap();

            Map<String, String> res = new HashMap<String, String>(map.entries.size());

            for (Pair<Term.Raw, Term.Raw> entry : map.entries)
            {
                // Because the parser tries to be smart and recover on error (to
                // allow displaying more than one error I suppose), we have null
                // entries in there. Just skip those, a proper error will be thrown in the end.
                if (entry.left == null || entry.right == null)
                    break;

                if (!(entry.left instanceof Constants.Literal))
                {
                    addRecognitionError("Invalid property name: " + entry.left);
                    break;
                }
                if (!(entry.right instanceof Constants.Literal))
                {
                    addRecognitionError("Invalid property value: " + entry.right);
                    break;
                }

                res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText());
            }

            return res;
        }

        public void addRawUpdate(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, Operation.RawUpdate update)
        {
            for (Pair<ColumnIdentifier, Operation.RawUpdate> p : operations)
            {
                if (p.left.equals(key) && !p.right.isCompatibleWith(update))
                    addRecognitionError("Multiple incompatible setting of column " + key);
            }
            operations.add(Pair.create(key, update));
        }



    // $ANTLR start "query"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:166:1: query returns [ParsedStatement stmnt] : st= cqlStatement ( ';' )* EOF ;
    public final ParsedStatement query() throws RecognitionException {
        ParsedStatement stmnt = null;

        ParsedStatement st = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:169:5: (st= cqlStatement ( ';' )* EOF )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:169:7: st= cqlStatement ( ';' )* EOF
            {
            pushFollow(FOLLOW_cqlStatement_in_query72);
            st=cqlStatement();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:169:23: ( ';' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==125) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:169:24: ';'
            	    {
            	    match(input,125,FOLLOW_125_in_query75); 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:172:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement );
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

        AlterKeyspaceStatement st15 = null;

        GrantStatement st16 = null;

        RevokeStatement st17 = null;

        ListPermissionsStatement st18 = null;

        CreateUserStatement st19 = null;

        AlterUserStatement st20 = null;

        DropUserStatement st21 = null;

        ListUsersStatement st22 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:174:5: (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement )
            int alt2=22;
            alt2 = dfa2.predict(input);
            switch (alt2) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:174:7: st1= selectStatement
                    {
                    pushFollow(FOLLOW_selectStatement_in_cqlStatement113);
                    st1=selectStatement();

                    state._fsp--;

                     stmt = st1; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:175:7: st2= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_cqlStatement138);
                    st2=insertStatement();

                    state._fsp--;

                     stmt = st2; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:176:7: st3= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_cqlStatement163);
                    st3=updateStatement();

                    state._fsp--;

                     stmt = st3; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:177:7: st4= batchStatement
                    {
                    pushFollow(FOLLOW_batchStatement_in_cqlStatement188);
                    st4=batchStatement();

                    state._fsp--;

                     stmt = st4; 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:178:7: st5= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_cqlStatement214);
                    st5=deleteStatement();

                    state._fsp--;

                     stmt = st5; 

                    }
                    break;
                case 6 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:179:7: st6= useStatement
                    {
                    pushFollow(FOLLOW_useStatement_in_cqlStatement239);
                    st6=useStatement();

                    state._fsp--;

                     stmt = st6; 

                    }
                    break;
                case 7 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:180:7: st7= truncateStatement
                    {
                    pushFollow(FOLLOW_truncateStatement_in_cqlStatement267);
                    st7=truncateStatement();

                    state._fsp--;

                     stmt = st7; 

                    }
                    break;
                case 8 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:181:7: st8= createKeyspaceStatement
                    {
                    pushFollow(FOLLOW_createKeyspaceStatement_in_cqlStatement290);
                    st8=createKeyspaceStatement();

                    state._fsp--;

                     stmt = st8; 

                    }
                    break;
                case 9 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:182:7: st9= createColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_createColumnFamilyStatement_in_cqlStatement307);
                    st9=createColumnFamilyStatement();

                    state._fsp--;

                     stmt = st9; 

                    }
                    break;
                case 10 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:183:7: st10= createIndexStatement
                    {
                    pushFollow(FOLLOW_createIndexStatement_in_cqlStatement319);
                    st10=createIndexStatement();

                    state._fsp--;

                     stmt = st10; 

                    }
                    break;
                case 11 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:184:7: st11= dropKeyspaceStatement
                    {
                    pushFollow(FOLLOW_dropKeyspaceStatement_in_cqlStatement338);
                    st11=dropKeyspaceStatement();

                    state._fsp--;

                     stmt = st11; 

                    }
                    break;
                case 12 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:185:7: st12= dropColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_dropColumnFamilyStatement_in_cqlStatement356);
                    st12=dropColumnFamilyStatement();

                    state._fsp--;

                     stmt = st12; 

                    }
                    break;
                case 13 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:186:7: st13= dropIndexStatement
                    {
                    pushFollow(FOLLOW_dropIndexStatement_in_cqlStatement370);
                    st13=dropIndexStatement();

                    state._fsp--;

                     stmt = st13; 

                    }
                    break;
                case 14 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:187:7: st14= alterTableStatement
                    {
                    pushFollow(FOLLOW_alterTableStatement_in_cqlStatement391);
                    st14=alterTableStatement();

                    state._fsp--;

                     stmt = st14; 

                    }
                    break;
                case 15 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:188:7: st15= alterKeyspaceStatement
                    {
                    pushFollow(FOLLOW_alterKeyspaceStatement_in_cqlStatement411);
                    st15=alterKeyspaceStatement();

                    state._fsp--;

                     stmt = st15; 

                    }
                    break;
                case 16 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:189:7: st16= grantStatement
                    {
                    pushFollow(FOLLOW_grantStatement_in_cqlStatement428);
                    st16=grantStatement();

                    state._fsp--;

                     stmt = st16; 

                    }
                    break;
                case 17 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:190:7: st17= revokeStatement
                    {
                    pushFollow(FOLLOW_revokeStatement_in_cqlStatement453);
                    st17=revokeStatement();

                    state._fsp--;

                     stmt = st17; 

                    }
                    break;
                case 18 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:191:7: st18= listPermissionsStatement
                    {
                    pushFollow(FOLLOW_listPermissionsStatement_in_cqlStatement477);
                    st18=listPermissionsStatement();

                    state._fsp--;

                     stmt = st18; 

                    }
                    break;
                case 19 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:192:7: st19= createUserStatement
                    {
                    pushFollow(FOLLOW_createUserStatement_in_cqlStatement492);
                    st19=createUserStatement();

                    state._fsp--;

                     stmt = st19; 

                    }
                    break;
                case 20 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:193:7: st20= alterUserStatement
                    {
                    pushFollow(FOLLOW_alterUserStatement_in_cqlStatement512);
                    st20=alterUserStatement();

                    state._fsp--;

                     stmt = st20; 

                    }
                    break;
                case 21 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:194:7: st21= dropUserStatement
                    {
                    pushFollow(FOLLOW_dropUserStatement_in_cqlStatement533);
                    st21=dropUserStatement();

                    state._fsp--;

                     stmt = st21; 

                    }
                    break;
                case 22 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:195:7: st22= listUsersStatement
                    {
                    pushFollow(FOLLOW_listUsersStatement_in_cqlStatement555);
                    st22=listUsersStatement();

                    state._fsp--;

                     stmt = st22; 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:201:1: useStatement returns [UseStatement stmt] : K_USE ks= keyspaceName ;
    public final UseStatement useStatement() throws RecognitionException {
        UseStatement stmt = null;

        String ks = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:202:5: ( K_USE ks= keyspaceName )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:202:7: K_USE ks= keyspaceName
            {
            match(input,K_USE,FOLLOW_K_USE_in_useStatement589); 
            pushFollow(FOLLOW_keyspaceName_in_useStatement593);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:205:1: selectStatement returns [SelectStatement.RawStatement expr] : K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )? ( K_ALLOW K_FILTERING )? ;
    public final SelectStatement.RawStatement selectStatement() throws RecognitionException {
        SelectStatement.RawStatement expr = null;

        Token rows=null;
        List<RawSelector> sclause = null;

        CFName cf = null;

        List<Relation> wclause = null;



                boolean isCount = false;
                int limit = Integer.MAX_VALUE;
                Map<ColumnIdentifier, Boolean> orderings = new LinkedHashMap<ColumnIdentifier, Boolean>();
                boolean allowFiltering = false;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:5: ( K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )? ( K_ALLOW K_FILTERING )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:7: K_SELECT (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= INTEGER )? ( K_ALLOW K_FILTERING )?
            {
            match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement627); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:16: (sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( ((LA3_0>=K_FILTERING && LA3_0<=K_TTL)||LA3_0==K_VALUES||LA3_0==K_TIMESTAMP||LA3_0==K_COUNTER||(LA3_0>=K_KEY && LA3_0<=K_CLUSTERING)||LA3_0==IDENT||LA3_0==K_TYPE||LA3_0==K_LIST||(LA3_0>=K_ALL && LA3_0<=K_PASSWORD)||LA3_0==QUOTED_NAME||LA3_0==K_TOKEN||(LA3_0>=K_ASCII && LA3_0<=K_MAP)||LA3_0==129) ) {
                alt3=1;
            }
            else if ( (LA3_0==K_COUNT) ) {
                int LA3_2 = input.LA(2);

                if ( (LA3_2==126) ) {
                    alt3=2;
                }
                else if ( (LA3_2==K_FROM||LA3_2==128) ) {
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
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:18: sclause= selectClause
                    {
                    pushFollow(FOLLOW_selectClause_in_selectStatement633);
                    sclause=selectClause();

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:41: ( K_COUNT '(' sclause= selectCountClause ')' )
                    {
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:41: ( K_COUNT '(' sclause= selectCountClause ')' )
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:218:42: K_COUNT '(' sclause= selectCountClause ')'
                    {
                    match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement638); 
                    match(input,126,FOLLOW_126_in_selectStatement640); 
                    pushFollow(FOLLOW_selectCountClause_in_selectStatement644);
                    sclause=selectCountClause();

                    state._fsp--;

                    match(input,127,FOLLOW_127_in_selectStatement646); 
                     isCount = true; 

                    }


                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement659); 
            pushFollow(FOLLOW_columnFamilyName_in_selectStatement663);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:220:7: ( K_WHERE wclause= whereClause )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==K_WHERE) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:220:9: K_WHERE wclause= whereClause
                    {
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement673); 
                    pushFollow(FOLLOW_whereClause_in_selectStatement677);
                    wclause=whereClause();

                    state._fsp--;


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:221:7: ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==K_ORDER) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:221:9: K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )*
                    {
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_selectStatement690); 
                    match(input,K_BY,FOLLOW_K_BY_in_selectStatement692); 
                    pushFollow(FOLLOW_orderByClause_in_selectStatement694);
                    orderByClause(orderings);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:221:47: ( ',' orderByClause[orderings] )*
                    loop5:
                    do {
                        int alt5=2;
                        int LA5_0 = input.LA(1);

                        if ( (LA5_0==128) ) {
                            alt5=1;
                        }


                        switch (alt5) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:221:49: ',' orderByClause[orderings]
                    	    {
                    	    match(input,128,FOLLOW_128_in_selectStatement699); 
                    	    pushFollow(FOLLOW_orderByClause_in_selectStatement701);
                    	    orderByClause(orderings);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop5;
                        }
                    } while (true);


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:222:7: ( K_LIMIT rows= INTEGER )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==K_LIMIT) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:222:9: K_LIMIT rows= INTEGER
                    {
                    match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement718); 
                    rows=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectStatement722); 
                     limit = Integer.parseInt((rows!=null?rows.getText():null)); 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:223:7: ( K_ALLOW K_FILTERING )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==K_ALLOW) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:223:9: K_ALLOW K_FILTERING
                    {
                    match(input,K_ALLOW,FOLLOW_K_ALLOW_in_selectStatement737); 
                    match(input,K_FILTERING,FOLLOW_K_FILTERING_in_selectStatement739); 
                     allowFiltering = true; 

                    }
                    break;

            }


                      SelectStatement.Parameters params = new SelectStatement.Parameters(limit,
                                                                                         orderings,
                                                                                         isCount,
                                                                                         allowFiltering);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:233:1: selectClause returns [List<RawSelector> expr] : (t1= selector ( ',' tN= selector )* | '\\*' );
    public final List<RawSelector> selectClause() throws RecognitionException {
        List<RawSelector> expr = null;

        RawSelector t1 = null;

        RawSelector tN = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:234:5: (t1= selector ( ',' tN= selector )* | '\\*' )
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==K_COUNT||(LA10_0>=K_FILTERING && LA10_0<=K_TTL)||LA10_0==K_VALUES||LA10_0==K_TIMESTAMP||LA10_0==K_COUNTER||(LA10_0>=K_KEY && LA10_0<=K_CLUSTERING)||LA10_0==IDENT||LA10_0==K_TYPE||LA10_0==K_LIST||(LA10_0>=K_ALL && LA10_0<=K_PASSWORD)||LA10_0==QUOTED_NAME||LA10_0==K_TOKEN||(LA10_0>=K_ASCII && LA10_0<=K_MAP)) ) {
                alt10=1;
            }
            else if ( (LA10_0==129) ) {
                alt10=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:234:7: t1= selector ( ',' tN= selector )*
                    {
                    pushFollow(FOLLOW_selector_in_selectClause776);
                    t1=selector();

                    state._fsp--;

                     expr = new ArrayList<RawSelector>(); expr.add(t1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:234:76: ( ',' tN= selector )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( (LA9_0==128) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:234:77: ',' tN= selector
                    	    {
                    	    match(input,128,FOLLOW_128_in_selectClause781); 
                    	    pushFollow(FOLLOW_selector_in_selectClause785);
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
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:235:7: '\\*'
                    {
                    match(input,129,FOLLOW_129_in_selectClause797); 
                     expr = Collections.<RawSelector>emptyList();

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


    // $ANTLR start "selectionFunctionArgs"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:238:1: selectionFunctionArgs returns [List<RawSelector> a] : ( '(' ')' | '(' s1= selector ( ',' sn= selector )* ')' );
    public final List<RawSelector> selectionFunctionArgs() throws RecognitionException {
        List<RawSelector> a = null;

        RawSelector s1 = null;

        RawSelector sn = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:239:5: ( '(' ')' | '(' s1= selector ( ',' sn= selector )* ')' )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==126) ) {
                int LA12_1 = input.LA(2);

                if ( (LA12_1==127) ) {
                    alt12=1;
                }
                else if ( (LA12_1==K_COUNT||(LA12_1>=K_FILTERING && LA12_1<=K_TTL)||LA12_1==K_VALUES||LA12_1==K_TIMESTAMP||LA12_1==K_COUNTER||(LA12_1>=K_KEY && LA12_1<=K_CLUSTERING)||LA12_1==IDENT||LA12_1==K_TYPE||LA12_1==K_LIST||(LA12_1>=K_ALL && LA12_1<=K_PASSWORD)||LA12_1==QUOTED_NAME||LA12_1==K_TOKEN||(LA12_1>=K_ASCII && LA12_1<=K_MAP)) ) {
                    alt12=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:239:7: '(' ')'
                    {
                    match(input,126,FOLLOW_126_in_selectionFunctionArgs820); 
                    match(input,127,FOLLOW_127_in_selectionFunctionArgs822); 
                     a = Collections.emptyList(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:240:7: '(' s1= selector ( ',' sn= selector )* ')'
                    {
                    match(input,126,FOLLOW_126_in_selectionFunctionArgs832); 
                    pushFollow(FOLLOW_selector_in_selectionFunctionArgs836);
                    s1=selector();

                    state._fsp--;

                     List<RawSelector> args = new ArrayList<RawSelector>(); args.add(s1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:241:11: ( ',' sn= selector )*
                    loop11:
                    do {
                        int alt11=2;
                        int LA11_0 = input.LA(1);

                        if ( (LA11_0==128) ) {
                            alt11=1;
                        }


                        switch (alt11) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:241:13: ',' sn= selector
                    	    {
                    	    match(input,128,FOLLOW_128_in_selectionFunctionArgs852); 
                    	    pushFollow(FOLLOW_selector_in_selectionFunctionArgs856);
                    	    sn=selector();

                    	    state._fsp--;

                    	     args.add(sn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop11;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_selectionFunctionArgs870); 
                     a = args; 

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
        return a;
    }
    // $ANTLR end "selectionFunctionArgs"


    // $ANTLR start "selector"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:245:1: selector returns [RawSelector s] : (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs );
    public final RawSelector selector() throws RecognitionException {
        RawSelector s = null;

        ColumnIdentifier c = null;

        String f = null;

        List<RawSelector> args = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:246:5: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs )
            int alt13=4;
            alt13 = dfa13.predict(input);
            switch (alt13) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:246:7: c= cident
                    {
                    pushFollow(FOLLOW_cident_in_selector895);
                    c=cident();

                    state._fsp--;

                     s = c; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:247:7: K_WRITETIME '(' c= cident ')'
                    {
                    match(input,K_WRITETIME,FOLLOW_K_WRITETIME_in_selector938); 
                    match(input,126,FOLLOW_126_in_selector940); 
                    pushFollow(FOLLOW_cident_in_selector944);
                    c=cident();

                    state._fsp--;

                    match(input,127,FOLLOW_127_in_selector946); 
                     s = new RawSelector.WritetimeOrTTL(c, true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:248:7: K_TTL '(' c= cident ')'
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_selector969); 
                    match(input,126,FOLLOW_126_in_selector977); 
                    pushFollow(FOLLOW_cident_in_selector981);
                    c=cident();

                    state._fsp--;

                    match(input,127,FOLLOW_127_in_selector983); 
                     s = new RawSelector.WritetimeOrTTL(c, false); 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:249:7: f= functionName args= selectionFunctionArgs
                    {
                    pushFollow(FOLLOW_functionName_in_selector1008);
                    f=functionName();

                    state._fsp--;

                    pushFollow(FOLLOW_selectionFunctionArgs_in_selector1012);
                    args=selectionFunctionArgs();

                    state._fsp--;

                     s = new RawSelector.WithFunction(f, args); 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:252:1: selectCountClause returns [List<RawSelector> expr] : ( '\\*' | i= INTEGER );
    public final List<RawSelector> selectCountClause() throws RecognitionException {
        List<RawSelector> expr = null;

        Token i=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:253:5: ( '\\*' | i= INTEGER )
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==129) ) {
                alt14=1;
            }
            else if ( (LA14_0==INTEGER) ) {
                alt14=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }
            switch (alt14) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:253:7: '\\*'
                    {
                    match(input,129,FOLLOW_129_in_selectCountClause1035); 
                     expr = Collections.<RawSelector>emptyList();

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:254:7: i= INTEGER
                    {
                    i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectCountClause1057); 
                     if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")"); expr = Collections.<RawSelector>emptyList();

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:257:1: whereClause returns [List<Relation> clause] : relation[$clause] ( K_AND relation[$clause] )* ;
    public final List<Relation> whereClause() throws RecognitionException {
        List<Relation> clause = null;

         clause = new ArrayList<Relation>(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:259:5: ( relation[$clause] ( K_AND relation[$clause] )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:259:7: relation[$clause] ( K_AND relation[$clause] )*
            {
            pushFollow(FOLLOW_relation_in_whereClause1093);
            relation(clause);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:259:25: ( K_AND relation[$clause] )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==K_AND) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:259:26: K_AND relation[$clause]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_whereClause1097); 
            	    pushFollow(FOLLOW_relation_in_whereClause1099);
            	    relation(clause);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop15;
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:262:1: orderByClause[Map<ColumnIdentifier, Boolean> orderings] : c= cident ( K_ASC | K_DESC )? ;
    public final void orderByClause(Map<ColumnIdentifier, Boolean> orderings) throws RecognitionException {
        ColumnIdentifier c = null;



                ColumnIdentifier orderBy = null;
                boolean reversed = false;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:267:5: (c= cident ( K_ASC | K_DESC )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:267:7: c= cident ( K_ASC | K_DESC )?
            {
            pushFollow(FOLLOW_cident_in_orderByClause1130);
            c=cident();

            state._fsp--;

             orderBy = c; 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:267:33: ( K_ASC | K_DESC )?
            int alt16=3;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==K_ASC) ) {
                alt16=1;
            }
            else if ( (LA16_0==K_DESC) ) {
                alt16=2;
            }
            switch (alt16) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:267:34: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_orderByClause1135); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:267:42: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_orderByClause1139); 
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:270:1: insertStatement returns [UpdateStatement expr] : K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( usingClause[attrs] )? ;
    public final UpdateStatement insertStatement() throws RecognitionException {
        UpdateStatement expr = null;

        CFName cf = null;

        ColumnIdentifier c1 = null;

        ColumnIdentifier cn = null;

        Term.Raw v1 = null;

        Term.Raw vn = null;



                Attributes attrs = new Attributes();
                List<ColumnIdentifier> columnNames  = new ArrayList<ColumnIdentifier>();
                List<Term.Raw> values = new ArrayList<Term.Raw>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:282:5: ( K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( usingClause[attrs] )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:282:7: K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( usingClause[attrs] )?
            {
            match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement1177); 
            match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement1179); 
            pushFollow(FOLLOW_columnFamilyName_in_insertStatement1183);
            cf=columnFamilyName();

            state._fsp--;

            match(input,126,FOLLOW_126_in_insertStatement1195); 
            pushFollow(FOLLOW_cident_in_insertStatement1199);
            c1=cident();

            state._fsp--;

             columnNames.add(c1); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:283:51: ( ',' cn= cident )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==128) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:283:53: ',' cn= cident
            	    {
            	    match(input,128,FOLLOW_128_in_insertStatement1206); 
            	    pushFollow(FOLLOW_cident_in_insertStatement1210);
            	    cn=cident();

            	    state._fsp--;

            	     columnNames.add(cn); 

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);

            match(input,127,FOLLOW_127_in_insertStatement1217); 
            match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement1227); 
            match(input,126,FOLLOW_126_in_insertStatement1239); 
            pushFollow(FOLLOW_term_in_insertStatement1243);
            v1=term();

            state._fsp--;

             values.add(v1); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:285:43: ( ',' vn= term )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( (LA18_0==128) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:285:45: ',' vn= term
            	    {
            	    match(input,128,FOLLOW_128_in_insertStatement1249); 
            	    pushFollow(FOLLOW_term_in_insertStatement1253);
            	    vn=term();

            	    state._fsp--;

            	     values.add(vn); 

            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);

            match(input,127,FOLLOW_127_in_insertStatement1260); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:286:9: ( usingClause[attrs] )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==K_USING) ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:286:11: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_insertStatement1272);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }


                      expr = new UpdateStatement(cf, attrs, columnNames, values);
                  

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:292:1: usingClause[Attributes attrs] : K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* ;
    public final void usingClause(Attributes attrs) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:5: ( K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:7: K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClause1302); 
            pushFollow(FOLLOW_usingClauseObjective_in_usingClause1304);
            usingClauseObjective(attrs);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:43: ( ( K_AND )? usingClauseObjective[attrs] )*
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( ((LA21_0>=K_TTL && LA21_0<=K_AND)||LA21_0==K_TIMESTAMP) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:45: ( K_AND )? usingClauseObjective[attrs]
            	    {
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:45: ( K_AND )?
            	    int alt20=2;
            	    int LA20_0 = input.LA(1);

            	    if ( (LA20_0==K_AND) ) {
            	        alt20=1;
            	    }
            	    switch (alt20) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:293:45: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClause1309); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseObjective_in_usingClause1312);
            	    usingClauseObjective(attrs);

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
    // $ANTLR end "usingClause"


    // $ANTLR start "usingClauseDelete"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:296:1: usingClauseDelete[Attributes attrs] : K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* ;
    public final void usingClauseDelete(Attributes attrs) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:5: ( K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:7: K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete1334); 
            pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1336);
            usingClauseDeleteObjective(attrs);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:49: ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            loop23:
            do {
                int alt23=2;
                int LA23_0 = input.LA(1);

                if ( (LA23_0==K_AND||LA23_0==K_TIMESTAMP) ) {
                    alt23=1;
                }


                switch (alt23) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:51: ( K_AND )? usingClauseDeleteObjective[attrs]
            	    {
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:51: ( K_AND )?
            	    int alt22=2;
            	    int LA22_0 = input.LA(1);

            	    if ( (LA22_0==K_AND) ) {
            	        alt22=1;
            	    }
            	    switch (alt22) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:297:51: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClauseDelete1341); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1344);
            	    usingClauseDeleteObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop23;
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:300:1: usingClauseDeleteObjective[Attributes attrs] : K_TIMESTAMP ts= INTEGER ;
    public final void usingClauseDeleteObjective(Attributes attrs) throws RecognitionException {
        Token ts=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:301:5: ( K_TIMESTAMP ts= INTEGER )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:301:7: K_TIMESTAMP ts= INTEGER
            {
            match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1366); 
            ts=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseDeleteObjective1370); 
             attrs.timestamp = Long.valueOf((ts!=null?ts.getText():null)); 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:304:1: usingClauseObjective[Attributes attrs] : ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER );
    public final void usingClauseObjective(Attributes attrs) throws RecognitionException {
        Token t=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:305:5: ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER )
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==K_TIMESTAMP) ) {
                alt24=1;
            }
            else if ( (LA24_0==K_TTL) ) {
                alt24=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 24, 0, input);

                throw nvae;
            }
            switch (alt24) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:305:7: usingClauseDeleteObjective[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1390);
                    usingClauseDeleteObjective(attrs);

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:306:7: K_TTL t= INTEGER
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective1399); 
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseObjective1403); 
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:309:1: updateStatement returns [UpdateStatement expr] : K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ;
    public final UpdateStatement updateStatement() throws RecognitionException {
        UpdateStatement expr = null;

        CFName cf = null;

        List<Relation> wclause = null;



                Attributes attrs = new Attributes();
                List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations = new ArrayList<Pair<ColumnIdentifier, Operation.RawUpdate>>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:320:5: ( K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:320:7: K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause
            {
            match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1437); 
            pushFollow(FOLLOW_columnFamilyName_in_updateStatement1441);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:321:7: ( usingClause[attrs] )?
            int alt25=2;
            int LA25_0 = input.LA(1);

            if ( (LA25_0==K_USING) ) {
                alt25=1;
            }
            switch (alt25) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:321:9: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_updateStatement1451);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_SET,FOLLOW_K_SET_in_updateStatement1463); 
            pushFollow(FOLLOW_columnOperation_in_updateStatement1465);
            columnOperation(operations);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:322:41: ( ',' columnOperation[operations] )*
            loop26:
            do {
                int alt26=2;
                int LA26_0 = input.LA(1);

                if ( (LA26_0==128) ) {
                    alt26=1;
                }


                switch (alt26) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:322:42: ',' columnOperation[operations]
            	    {
            	    match(input,128,FOLLOW_128_in_updateStatement1469); 
            	    pushFollow(FOLLOW_columnOperation_in_updateStatement1471);
            	    columnOperation(operations);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop26;
                }
            } while (true);

            match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1482); 
            pushFollow(FOLLOW_whereClause_in_updateStatement1486);
            wclause=whereClause();

            state._fsp--;


                      return new UpdateStatement(cf, operations, wclause, attrs);
                  

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:329:1: deleteStatement returns [DeleteStatement expr] : K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ;
    public final DeleteStatement deleteStatement() throws RecognitionException {
        DeleteStatement expr = null;

        List<Operation.RawDeletion> dels = null;

        CFName cf = null;

        List<Relation> wclause = null;



                Attributes attrs = new Attributes();
                List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:340:5: ( K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:340:7: K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause
            {
            match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1526); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:340:16: (dels= deleteSelection )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==K_COUNT||(LA27_0>=K_FILTERING && LA27_0<=K_TTL)||LA27_0==K_VALUES||LA27_0==K_TIMESTAMP||LA27_0==K_COUNTER||(LA27_0>=K_KEY && LA27_0<=K_CLUSTERING)||LA27_0==IDENT||LA27_0==K_TYPE||LA27_0==K_LIST||(LA27_0>=K_ALL && LA27_0<=K_PASSWORD)||LA27_0==QUOTED_NAME||(LA27_0>=K_ASCII && LA27_0<=K_MAP)) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:340:18: dels= deleteSelection
                    {
                    pushFollow(FOLLOW_deleteSelection_in_deleteStatement1532);
                    dels=deleteSelection();

                    state._fsp--;

                     columnDeletions = dels; 

                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1545); 
            pushFollow(FOLLOW_columnFamilyName_in_deleteStatement1549);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:342:7: ( usingClauseDelete[attrs] )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==K_USING) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:342:9: usingClauseDelete[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1559);
                    usingClauseDelete(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1571); 
            pushFollow(FOLLOW_whereClause_in_deleteStatement1575);
            wclause=whereClause();

            state._fsp--;


                      return new DeleteStatement(cf, columnDeletions, wclause, attrs);
                  

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


    // $ANTLR start "deleteSelection"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:349:1: deleteSelection returns [List<Operation.RawDeletion> operations] : t1= deleteOp ( ',' tN= deleteOp )* ;
    public final List<Operation.RawDeletion> deleteSelection() throws RecognitionException {
        List<Operation.RawDeletion> operations = null;

        Operation.RawDeletion t1 = null;

        Operation.RawDeletion tN = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:350:5: (t1= deleteOp ( ',' tN= deleteOp )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:350:7: t1= deleteOp ( ',' tN= deleteOp )*
            {
             operations = new ArrayList<Operation.RawDeletion>(); 
            pushFollow(FOLLOW_deleteOp_in_deleteSelection1618);
            t1=deleteOp();

            state._fsp--;

             operations.add(t1); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:352:11: ( ',' tN= deleteOp )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( (LA29_0==128) ) {
                    alt29=1;
                }


                switch (alt29) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:352:12: ',' tN= deleteOp
            	    {
            	    match(input,128,FOLLOW_128_in_deleteSelection1633); 
            	    pushFollow(FOLLOW_deleteOp_in_deleteSelection1637);
            	    tN=deleteOp();

            	    state._fsp--;

            	     operations.add(tN); 

            	    }
            	    break;

            	default :
            	    break loop29;
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
        return operations;
    }
    // $ANTLR end "deleteSelection"


    // $ANTLR start "deleteOp"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:355:1: deleteOp returns [Operation.RawDeletion op] : (c= cident | c= cident '[' t= term ']' );
    public final Operation.RawDeletion deleteOp() throws RecognitionException {
        Operation.RawDeletion op = null;

        ColumnIdentifier c = null;

        Term.Raw t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:356:5: (c= cident | c= cident '[' t= term ']' )
            int alt30=2;
            alt30 = dfa30.predict(input);
            switch (alt30) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:356:7: c= cident
                    {
                    pushFollow(FOLLOW_cident_in_deleteOp1664);
                    c=cident();

                    state._fsp--;

                     op = new Operation.ColumnDeletion(c); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:357:7: c= cident '[' t= term ']'
                    {
                    pushFollow(FOLLOW_cident_in_deleteOp1691);
                    c=cident();

                    state._fsp--;

                    match(input,130,FOLLOW_130_in_deleteOp1693); 
                    pushFollow(FOLLOW_term_in_deleteOp1697);
                    t=term();

                    state._fsp--;

                    match(input,131,FOLLOW_131_in_deleteOp1699); 
                     op = new Operation.ElementDeletion(c, t); 

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
        return op;
    }
    // $ANTLR end "deleteOp"


    // $ANTLR start "batchStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:360:1: batchStatement returns [BatchStatement expr] : K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH ;
    public final BatchStatement batchStatement() throws RecognitionException {
        BatchStatement expr = null;

        ModificationStatement s1 = null;

        ModificationStatement sN = null;



                BatchStatement.Type type = BatchStatement.Type.LOGGED;
                List<ModificationStatement> statements = new ArrayList<ModificationStatement>();
                Attributes attrs = new Attributes();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:390:5: ( K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:390:7: K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH
            {
            match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement1733); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:391:7: ( K_UNLOGGED | K_COUNTER )?
            int alt31=3;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==K_UNLOGGED) ) {
                alt31=1;
            }
            else if ( (LA31_0==K_COUNTER) ) {
                alt31=2;
            }
            switch (alt31) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:391:9: K_UNLOGGED
                    {
                    match(input,K_UNLOGGED,FOLLOW_K_UNLOGGED_in_batchStatement1743); 
                     type = BatchStatement.Type.UNLOGGED; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:391:63: K_COUNTER
                    {
                    match(input,K_COUNTER,FOLLOW_K_COUNTER_in_batchStatement1749); 
                     type = BatchStatement.Type.COUNTER; 

                    }
                    break;

            }

            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1762); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:392:15: ( usingClause[attrs] )?
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==K_USING) ) {
                alt32=1;
            }
            switch (alt32) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:392:17: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_batchStatement1766);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1784);
            s1=batchStatementObjective();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:38: ( ';' )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==125) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:38: ';'
                    {
                    match(input,125,FOLLOW_125_in_batchStatement1786); 

                    }
                    break;

            }

             statements.add(s1); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:67: (sN= batchStatementObjective ( ';' )? )*
            loop35:
            do {
                int alt35=2;
                int LA35_0 = input.LA(1);

                if ( (LA35_0==K_INSERT||LA35_0==K_UPDATE||LA35_0==K_DELETE) ) {
                    alt35=1;
                }


                switch (alt35) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:69: sN= batchStatementObjective ( ';' )?
            	    {
            	    pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1795);
            	    sN=batchStatementObjective();

            	    state._fsp--;

            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:96: ( ';' )?
            	    int alt34=2;
            	    int LA34_0 = input.LA(1);

            	    if ( (LA34_0==125) ) {
            	        alt34=1;
            	    }
            	    switch (alt34) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:393:96: ';'
            	            {
            	            match(input,125,FOLLOW_125_in_batchStatement1797); 

            	            }
            	            break;

            	    }

            	     statements.add(sN); 

            	    }
            	    break;

            	default :
            	    break loop35;
                }
            } while (true);

            match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement1811); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1813); 

                      return new BatchStatement(type, statements, attrs);
                  

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:400:1: batchStatementObjective returns [ModificationStatement statement] : (i= insertStatement | u= updateStatement | d= deleteStatement );
    public final ModificationStatement batchStatementObjective() throws RecognitionException {
        ModificationStatement statement = null;

        UpdateStatement i = null;

        UpdateStatement u = null;

        DeleteStatement d = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:401:5: (i= insertStatement | u= updateStatement | d= deleteStatement )
            int alt36=3;
            switch ( input.LA(1) ) {
            case K_INSERT:
                {
                alt36=1;
                }
                break;
            case K_UPDATE:
                {
                alt36=2;
                }
                break;
            case K_DELETE:
                {
                alt36=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 36, 0, input);

                throw nvae;
            }

            switch (alt36) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:401:7: i= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_batchStatementObjective1844);
                    i=insertStatement();

                    state._fsp--;

                     statement = i; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:402:7: u= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_batchStatementObjective1857);
                    u=updateStatement();

                    state._fsp--;

                     statement = u; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:403:7: d= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective1870);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:406:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
    public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
        CreateKeyspaceStatement expr = null;

        String ks = null;


         KSPropDefs attrs = new KSPropDefs(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:411:5: ( K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:411:7: K_CREATE K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement1905); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1907); 
            pushFollow(FOLLOW_keyspaceName_in_createKeyspaceStatement1911);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement1919); 
            pushFollow(FOLLOW_properties_in_createKeyspaceStatement1921);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:415:1: createColumnFamilyStatement returns [CreateColumnFamilyStatement.RawStatement expr] : K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr] ;
    public final CreateColumnFamilyStatement.RawStatement createColumnFamilyStatement() throws RecognitionException {
        CreateColumnFamilyStatement.RawStatement expr = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:423:5: ( K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:423:7: K_CREATE K_COLUMNFAMILY cf= columnFamilyName cfamDefinition[expr]
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createColumnFamilyStatement1947); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1949); 
            pushFollow(FOLLOW_columnFamilyName_in_createColumnFamilyStatement1953);
            cf=columnFamilyName();

            state._fsp--;

             expr = new CreateColumnFamilyStatement.RawStatement(cf); 
            pushFollow(FOLLOW_cfamDefinition_in_createColumnFamilyStatement1963);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:427:1: cfamDefinition[CreateColumnFamilyStatement.RawStatement expr] : '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? ;
    public final void cfamDefinition(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:5: ( '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:7: '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            {
            match(input,126,FOLLOW_126_in_cfamDefinition1982); 
            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition1984);
            cfamColumns(expr);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:29: ( ',' ( cfamColumns[expr] )? )*
            loop38:
            do {
                int alt38=2;
                int LA38_0 = input.LA(1);

                if ( (LA38_0==128) ) {
                    alt38=1;
                }


                switch (alt38) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:31: ',' ( cfamColumns[expr] )?
            	    {
            	    match(input,128,FOLLOW_128_in_cfamDefinition1989); 
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:35: ( cfamColumns[expr] )?
            	    int alt37=2;
            	    int LA37_0 = input.LA(1);

            	    if ( (LA37_0==K_COUNT||(LA37_0>=K_FILTERING && LA37_0<=K_TTL)||LA37_0==K_VALUES||LA37_0==K_TIMESTAMP||LA37_0==K_COUNTER||(LA37_0>=K_PRIMARY && LA37_0<=K_CLUSTERING)||LA37_0==IDENT||LA37_0==K_TYPE||LA37_0==K_LIST||(LA37_0>=K_ALL && LA37_0<=K_PASSWORD)||LA37_0==QUOTED_NAME||(LA37_0>=K_ASCII && LA37_0<=K_MAP)) ) {
            	        alt37=1;
            	    }
            	    switch (alt37) {
            	        case 1 :
            	            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:428:35: cfamColumns[expr]
            	            {
            	            pushFollow(FOLLOW_cfamColumns_in_cfamDefinition1991);
            	            cfamColumns(expr);

            	            state._fsp--;


            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    break loop38;
                }
            } while (true);

            match(input,127,FOLLOW_127_in_cfamDefinition1998); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:429:7: ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==K_WITH) ) {
                alt40=1;
            }
            switch (alt40) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:429:9: K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_cfamDefinition2008); 
                    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2010);
                    cfamProperty(expr);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:429:35: ( K_AND cfamProperty[expr] )*
                    loop39:
                    do {
                        int alt39=2;
                        int LA39_0 = input.LA(1);

                        if ( (LA39_0==K_AND) ) {
                            alt39=1;
                        }


                        switch (alt39) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:429:37: K_AND cfamProperty[expr]
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_cfamDefinition2015); 
                    	    pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2017);
                    	    cfamProperty(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop39;
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:432:1: cfamColumns[CreateColumnFamilyStatement.RawStatement expr] : (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')' );
    public final void cfamColumns(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        CQL3Type v = null;

        ColumnIdentifier c = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:433:5: (k= cident v= comparatorType ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')' )
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==K_COUNT||(LA43_0>=K_FILTERING && LA43_0<=K_TTL)||LA43_0==K_VALUES||LA43_0==K_TIMESTAMP||LA43_0==K_COUNTER||(LA43_0>=K_KEY && LA43_0<=K_CLUSTERING)||LA43_0==IDENT||LA43_0==K_TYPE||LA43_0==K_LIST||(LA43_0>=K_ALL && LA43_0<=K_PASSWORD)||LA43_0==QUOTED_NAME||(LA43_0>=K_ASCII && LA43_0<=K_MAP)) ) {
                alt43=1;
            }
            else if ( (LA43_0==K_PRIMARY) ) {
                alt43=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 43, 0, input);

                throw nvae;
            }
            switch (alt43) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:433:7: k= cident v= comparatorType ( K_PRIMARY K_KEY )?
                    {
                    pushFollow(FOLLOW_cident_in_cfamColumns2043);
                    k=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_cfamColumns2047);
                    v=comparatorType();

                    state._fsp--;

                     expr.addDefinition(k, v); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:433:64: ( K_PRIMARY K_KEY )?
                    int alt41=2;
                    int LA41_0 = input.LA(1);

                    if ( (LA41_0==K_PRIMARY) ) {
                        alt41=1;
                    }
                    switch (alt41) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:433:65: K_PRIMARY K_KEY
                            {
                            match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2052); 
                            match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2054); 
                             expr.addKeyAliases(Collections.singletonList(k)); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:434:7: K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= cident )* ')'
                    {
                    match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2066); 
                    match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2068); 
                    match(input,126,FOLLOW_126_in_cfamColumns2070); 
                    pushFollow(FOLLOW_pkDef_in_cfamColumns2072);
                    pkDef(expr);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:434:39: ( ',' c= cident )*
                    loop42:
                    do {
                        int alt42=2;
                        int LA42_0 = input.LA(1);

                        if ( (LA42_0==128) ) {
                            alt42=1;
                        }


                        switch (alt42) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:434:40: ',' c= cident
                    	    {
                    	    match(input,128,FOLLOW_128_in_cfamColumns2076); 
                    	    pushFollow(FOLLOW_cident_in_cfamColumns2080);
                    	    c=cident();

                    	    state._fsp--;

                    	     expr.addColumnAlias(c); 

                    	    }
                    	    break;

                    	default :
                    	    break loop42;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_cfamColumns2087); 

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


    // $ANTLR start "pkDef"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:437:1: pkDef[CreateColumnFamilyStatement.RawStatement expr] : (k= cident | '(' k1= cident ( ',' kn= cident )* ')' );
    public final void pkDef(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;

        ColumnIdentifier k1 = null;

        ColumnIdentifier kn = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:438:5: (k= cident | '(' k1= cident ( ',' kn= cident )* ')' )
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==K_COUNT||(LA45_0>=K_FILTERING && LA45_0<=K_TTL)||LA45_0==K_VALUES||LA45_0==K_TIMESTAMP||LA45_0==K_COUNTER||(LA45_0>=K_KEY && LA45_0<=K_CLUSTERING)||LA45_0==IDENT||LA45_0==K_TYPE||LA45_0==K_LIST||(LA45_0>=K_ALL && LA45_0<=K_PASSWORD)||LA45_0==QUOTED_NAME||(LA45_0>=K_ASCII && LA45_0<=K_MAP)) ) {
                alt45=1;
            }
            else if ( (LA45_0==126) ) {
                alt45=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 45, 0, input);

                throw nvae;
            }
            switch (alt45) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:438:7: k= cident
                    {
                    pushFollow(FOLLOW_cident_in_pkDef2107);
                    k=cident();

                    state._fsp--;

                     expr.addKeyAliases(Collections.singletonList(k)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:439:7: '(' k1= cident ( ',' kn= cident )* ')'
                    {
                    match(input,126,FOLLOW_126_in_pkDef2117); 
                     List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); 
                    pushFollow(FOLLOW_cident_in_pkDef2123);
                    k1=cident();

                    state._fsp--;

                     l.add(k1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:439:102: ( ',' kn= cident )*
                    loop44:
                    do {
                        int alt44=2;
                        int LA44_0 = input.LA(1);

                        if ( (LA44_0==128) ) {
                            alt44=1;
                        }


                        switch (alt44) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:439:104: ',' kn= cident
                    	    {
                    	    match(input,128,FOLLOW_128_in_pkDef2129); 
                    	    pushFollow(FOLLOW_cident_in_pkDef2133);
                    	    kn=cident();

                    	    state._fsp--;

                    	     l.add(kn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop44;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_pkDef2140); 
                     expr.addKeyAliases(l); 

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
    // $ANTLR end "pkDef"


    // $ANTLR start "cfamProperty"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:442:1: cfamProperty[CreateColumnFamilyStatement.RawStatement expr] : ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' );
    public final void cfamProperty(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:443:5: ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' )
            int alt47=3;
            switch ( input.LA(1) ) {
            case K_COUNT:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_STORAGE:
            case IDENT:
            case K_TYPE:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case QUOTED_NAME:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt47=1;
                }
                break;
            case K_COMPACT:
                {
                int LA47_2 = input.LA(2);

                if ( (LA47_2==K_STORAGE) ) {
                    alt47=2;
                }
                else if ( (LA47_2==136) ) {
                    alt47=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 2, input);

                    throw nvae;
                }
                }
                break;
            case K_CLUSTERING:
                {
                int LA47_3 = input.LA(2);

                if ( (LA47_3==K_ORDER) ) {
                    alt47=3;
                }
                else if ( (LA47_3==136) ) {
                    alt47=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 3, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 47, 0, input);

                throw nvae;
            }

            switch (alt47) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:443:7: property[expr.properties]
                    {
                    pushFollow(FOLLOW_property_in_cfamProperty2160);
                    property(expr.properties);

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:444:7: K_COMPACT K_STORAGE
                    {
                    match(input,K_COMPACT,FOLLOW_K_COMPACT_in_cfamProperty2169); 
                    match(input,K_STORAGE,FOLLOW_K_STORAGE_in_cfamProperty2171); 
                     expr.setCompactStorage(); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:445:7: K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')'
                    {
                    match(input,K_CLUSTERING,FOLLOW_K_CLUSTERING_in_cfamProperty2181); 
                    match(input,K_ORDER,FOLLOW_K_ORDER_in_cfamProperty2183); 
                    match(input,K_BY,FOLLOW_K_BY_in_cfamProperty2185); 
                    match(input,126,FOLLOW_126_in_cfamProperty2187); 
                    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2189);
                    cfamOrdering(expr);

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:445:56: ( ',' cfamOrdering[expr] )*
                    loop46:
                    do {
                        int alt46=2;
                        int LA46_0 = input.LA(1);

                        if ( (LA46_0==128) ) {
                            alt46=1;
                        }


                        switch (alt46) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:445:57: ',' cfamOrdering[expr]
                    	    {
                    	    match(input,128,FOLLOW_128_in_cfamProperty2193); 
                    	    pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2195);
                    	    cfamOrdering(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop46;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_cfamProperty2200); 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:448:1: cfamOrdering[CreateColumnFamilyStatement.RawStatement expr] : k= cident ( K_ASC | K_DESC ) ;
    public final void cfamOrdering(CreateColumnFamilyStatement.RawStatement expr) throws RecognitionException {
        ColumnIdentifier k = null;


         boolean reversed=false; 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:5: (k= cident ( K_ASC | K_DESC ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:7: k= cident ( K_ASC | K_DESC )
            {
            pushFollow(FOLLOW_cident_in_cfamOrdering2228);
            k=cident();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:16: ( K_ASC | K_DESC )
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==K_ASC) ) {
                alt48=1;
            }
            else if ( (LA48_0==K_DESC) ) {
                alt48=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 48, 0, input);

                throw nvae;
            }
            switch (alt48) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:17: K_ASC
                    {
                    match(input,K_ASC,FOLLOW_K_ASC_in_cfamOrdering2231); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:450:25: K_DESC
                    {
                    match(input,K_DESC,FOLLOW_K_DESC_in_cfamOrdering2235); 
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:453:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' ;
    public final CreateIndexStatement createIndexStatement() throws RecognitionException {
        CreateIndexStatement expr = null;

        Token idxName=null;
        CFName cf = null;

        ColumnIdentifier id = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:457:5: ( K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:457:7: K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= columnFamilyName '(' id= cident ')'
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement2264); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement2266); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:457:24: (idxName= IDENT )?
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( (LA49_0==IDENT) ) {
                alt49=1;
            }
            switch (alt49) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:457:25: idxName= IDENT
                    {
                    idxName=(Token)match(input,IDENT,FOLLOW_IDENT_in_createIndexStatement2271); 

                    }
                    break;

            }

            match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement2275); 
            pushFollow(FOLLOW_columnFamilyName_in_createIndexStatement2279);
            cf=columnFamilyName();

            state._fsp--;

            match(input,126,FOLLOW_126_in_createIndexStatement2281); 
            pushFollow(FOLLOW_cident_in_createIndexStatement2285);
            id=cident();

            state._fsp--;

            match(input,127,FOLLOW_127_in_createIndexStatement2287); 
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:461:1: alterKeyspaceStatement returns [AlterKeyspaceStatement expr] : K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
    public final AlterKeyspaceStatement alterKeyspaceStatement() throws RecognitionException {
        AlterKeyspaceStatement expr = null;

        String ks = null;


         KSPropDefs attrs = new KSPropDefs(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:466:5: ( K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:466:7: K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterKeyspaceStatement2327); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement2329); 
            pushFollow(FOLLOW_keyspaceName_in_alterKeyspaceStatement2333);
            ks=keyspaceName();

            state._fsp--;

            match(input,K_WITH,FOLLOW_K_WITH_in_alterKeyspaceStatement2343); 
            pushFollow(FOLLOW_properties_in_alterKeyspaceStatement2345);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:471:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) ;
    public final AlterTableStatement alterTableStatement() throws RecognitionException {
        AlterTableStatement expr = null;

        CFName cf = null;

        ColumnIdentifier id = null;

        CQL3Type v = null;

        ColumnIdentifier id1 = null;

        ColumnIdentifier toId1 = null;

        ColumnIdentifier idn = null;

        ColumnIdentifier toIdn = null;



                AlterTableStatement.Type type = null;
                CFPropDefs props = new CFPropDefs();
                Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>();
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:484:5: ( K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:484:7: K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2381); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2383); 
            pushFollow(FOLLOW_columnFamilyName_in_alterTableStatement2387);
            cf=columnFamilyName();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:485:11: ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
            int alt51=4;
            switch ( input.LA(1) ) {
            case K_ALTER:
                {
                alt51=1;
                }
                break;
            case K_ADD:
                {
                alt51=2;
                }
                break;
            case K_WITH:
                {
                alt51=3;
                }
                break;
            case K_RENAME:
                {
                alt51=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 51, 0, input);

                throw nvae;
            }

            switch (alt51) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:485:13: K_ALTER id= cident K_TYPE v= comparatorType
                    {
                    match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2401); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2405);
                    id=cident();

                    state._fsp--;

                    match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement2407); 
                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2411);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ALTER; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:486:13: K_ADD id= cident v= comparatorType
                    {
                    match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement2427); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2433);
                    id=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2437);
                    v=comparatorType();

                    state._fsp--;

                     type = AlterTableStatement.Type.ADD; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:488:13: K_WITH properties[props]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_alterTableStatement2471); 
                    pushFollow(FOLLOW_properties_in_alterTableStatement2474);
                    properties(props);

                    state._fsp--;

                     type = AlterTableStatement.Type.OPTS; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:489:13: K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )*
                    {
                    match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTableStatement2507); 
                     type = AlterTableStatement.Type.RENAME; 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2561);
                    id1=cident();

                    state._fsp--;

                    match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement2563); 
                    pushFollow(FOLLOW_cident_in_alterTableStatement2567);
                    toId1=cident();

                    state._fsp--;

                     renames.put(id1, toId1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:491:16: ( K_AND idn= cident K_TO toIdn= cident )*
                    loop50:
                    do {
                        int alt50=2;
                        int LA50_0 = input.LA(1);

                        if ( (LA50_0==K_AND) ) {
                            alt50=1;
                        }


                        switch (alt50) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:491:18: K_AND idn= cident K_TO toIdn= cident
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_alterTableStatement2588); 
                    	    pushFollow(FOLLOW_cident_in_alterTableStatement2592);
                    	    idn=cident();

                    	    state._fsp--;

                    	    match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement2594); 
                    	    pushFollow(FOLLOW_cident_in_alterTableStatement2598);
                    	    toIdn=cident();

                    	    state._fsp--;

                    	     renames.put(idn, toIdn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop50;
                        }
                    } while (true);


                    }
                    break;

            }


                    expr = new AlterTableStatement(cf, type, id, v, props, renames);
                

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:498:1: dropKeyspaceStatement returns [DropKeyspaceStatement ksp] : K_DROP K_KEYSPACE ks= keyspaceName ;
    public final DropKeyspaceStatement dropKeyspaceStatement() throws RecognitionException {
        DropKeyspaceStatement ksp = null;

        String ks = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:502:5: ( K_DROP K_KEYSPACE ks= keyspaceName )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:502:7: K_DROP K_KEYSPACE ks= keyspaceName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement2644); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2646); 
            pushFollow(FOLLOW_keyspaceName_in_dropKeyspaceStatement2650);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:505:1: dropColumnFamilyStatement returns [DropColumnFamilyStatement stmt] : K_DROP K_COLUMNFAMILY cf= columnFamilyName ;
    public final DropColumnFamilyStatement dropColumnFamilyStatement() throws RecognitionException {
        DropColumnFamilyStatement stmt = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:509:5: ( K_DROP K_COLUMNFAMILY cf= columnFamilyName )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:509:7: K_DROP K_COLUMNFAMILY cf= columnFamilyName
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropColumnFamilyStatement2675); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2677); 
            pushFollow(FOLLOW_columnFamilyName_in_dropColumnFamilyStatement2681);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:512:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX index= IDENT ;
    public final DropIndexStatement dropIndexStatement() throws RecognitionException {
        DropIndexStatement expr = null;

        Token index=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:516:5: ( K_DROP K_INDEX index= IDENT )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:517:7: K_DROP K_INDEX index= IDENT
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement2712); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement2714); 
            index=(Token)match(input,IDENT,FOLLOW_IDENT_in_dropIndexStatement2718); 
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:521:1: truncateStatement returns [TruncateStatement stmt] : K_TRUNCATE cf= columnFamilyName ;
    public final TruncateStatement truncateStatement() throws RecognitionException {
        TruncateStatement stmt = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:525:5: ( K_TRUNCATE cf= columnFamilyName )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:525:7: K_TRUNCATE cf= columnFamilyName
            {
            match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement2749); 
            pushFollow(FOLLOW_columnFamilyName_in_truncateStatement2753);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:528:1: grantStatement returns [GrantStatement stmt] : K_GRANT permissionOrAll K_ON resource K_TO username ;
    public final GrantStatement grantStatement() throws RecognitionException {
        GrantStatement stmt = null;

        Set<Permission> permissionOrAll1 = null;

        IResource resource2 = null;

        CqlParser.username_return username3 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:532:5: ( K_GRANT permissionOrAll K_ON resource K_TO username )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:532:7: K_GRANT permissionOrAll K_ON resource K_TO username
            {
            match(input,K_GRANT,FOLLOW_K_GRANT_in_grantStatement2778); 
            pushFollow(FOLLOW_permissionOrAll_in_grantStatement2790);
            permissionOrAll1=permissionOrAll();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_grantStatement2798); 
            pushFollow(FOLLOW_resource_in_grantStatement2810);
            resource2=resource();

            state._fsp--;

            match(input,K_TO,FOLLOW_K_TO_in_grantStatement2818); 
            pushFollow(FOLLOW_username_in_grantStatement2830);
            username3=username();

            state._fsp--;

             stmt = new GrantStatement(permissionOrAll1, resource2, (username3!=null?input.toString(username3.start,username3.stop):null)); 

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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:541:1: revokeStatement returns [RevokeStatement stmt] : K_REVOKE permissionOrAll K_ON resource K_FROM username ;
    public final RevokeStatement revokeStatement() throws RecognitionException {
        RevokeStatement stmt = null;

        Set<Permission> permissionOrAll4 = null;

        IResource resource5 = null;

        CqlParser.username_return username6 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:545:5: ( K_REVOKE permissionOrAll K_ON resource K_FROM username )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:545:7: K_REVOKE permissionOrAll K_ON resource K_FROM username
            {
            match(input,K_REVOKE,FOLLOW_K_REVOKE_in_revokeStatement2861); 
            pushFollow(FOLLOW_permissionOrAll_in_revokeStatement2873);
            permissionOrAll4=permissionOrAll();

            state._fsp--;

            match(input,K_ON,FOLLOW_K_ON_in_revokeStatement2881); 
            pushFollow(FOLLOW_resource_in_revokeStatement2893);
            resource5=resource();

            state._fsp--;

            match(input,K_FROM,FOLLOW_K_FROM_in_revokeStatement2901); 
            pushFollow(FOLLOW_username_in_revokeStatement2913);
            username6=username();

            state._fsp--;

             stmt = new RevokeStatement(permissionOrAll4, resource5, (username6!=null?input.toString(username6.start,username6.stop):null)); 

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


    // $ANTLR start "listPermissionsStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:554:1: listPermissionsStatement returns [ListPermissionsStatement stmt] : K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? ;
    public final ListPermissionsStatement listPermissionsStatement() throws RecognitionException {
        ListPermissionsStatement stmt = null;

        IResource resource7 = null;

        CqlParser.username_return username8 = null;

        Set<Permission> permissionOrAll9 = null;



                IResource resource = null;
                String username = null;
                boolean recursive = true;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:560:5: ( K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:560:7: K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )?
            {
            match(input,K_LIST,FOLLOW_K_LIST_in_listPermissionsStatement2951); 
            pushFollow(FOLLOW_permissionOrAll_in_listPermissionsStatement2963);
            permissionOrAll9=permissionOrAll();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:562:7: ( K_ON resource )?
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==K_ON) ) {
                alt52=1;
            }
            switch (alt52) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:562:9: K_ON resource
                    {
                    match(input,K_ON,FOLLOW_K_ON_in_listPermissionsStatement2973); 
                    pushFollow(FOLLOW_resource_in_listPermissionsStatement2975);
                    resource7=resource();

                    state._fsp--;

                     resource = resource7; 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:563:7: ( K_OF username )?
            int alt53=2;
            int LA53_0 = input.LA(1);

            if ( (LA53_0==K_OF) ) {
                alt53=1;
            }
            switch (alt53) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:563:9: K_OF username
                    {
                    match(input,K_OF,FOLLOW_K_OF_in_listPermissionsStatement2990); 
                    pushFollow(FOLLOW_username_in_listPermissionsStatement2992);
                    username8=username();

                    state._fsp--;

                     username = (username8!=null?input.toString(username8.start,username8.stop):null); 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:564:7: ( K_NORECURSIVE )?
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==K_NORECURSIVE) ) {
                alt54=1;
            }
            switch (alt54) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:564:9: K_NORECURSIVE
                    {
                    match(input,K_NORECURSIVE,FOLLOW_K_NORECURSIVE_in_listPermissionsStatement3007); 
                     recursive = false; 

                    }
                    break;

            }

             stmt = new ListPermissionsStatement(permissionOrAll9, resource, username, recursive); 

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
    // $ANTLR end "listPermissionsStatement"


    // $ANTLR start "permission"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:568:1: permission returns [Permission perm] : p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) ;
    public final Permission permission() throws RecognitionException {
        Permission perm = null;

        Token p=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:569:5: (p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:569:7: p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE )
            {
            p=(Token)input.LT(1);
            if ( input.LA(1)==K_SELECT||input.LA(1)==K_CREATE||input.LA(1)==K_ALTER||input.LA(1)==K_DROP||(input.LA(1)>=K_MODIFY && input.LA(1)<=K_AUTHORIZE) ) {
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


    // $ANTLR start "permissionOrAll"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:573:1: permissionOrAll returns [Set<Permission> perms] : ( K_ALL ( K_PERMISSIONS )? | p= permission ( K_PERMISSION )? );
    public final Set<Permission> permissionOrAll() throws RecognitionException {
        Set<Permission> perms = null;

        Permission p = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:5: ( K_ALL ( K_PERMISSIONS )? | p= permission ( K_PERMISSION )? )
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==K_ALL) ) {
                alt57=1;
            }
            else if ( (LA57_0==K_SELECT||LA57_0==K_CREATE||LA57_0==K_ALTER||LA57_0==K_DROP||(LA57_0>=K_MODIFY && LA57_0<=K_AUTHORIZE)) ) {
                alt57=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 57, 0, input);

                throw nvae;
            }
            switch (alt57) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:7: K_ALL ( K_PERMISSIONS )?
                    {
                    match(input,K_ALL,FOLLOW_K_ALL_in_permissionOrAll3092); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:13: ( K_PERMISSIONS )?
                    int alt55=2;
                    int LA55_0 = input.LA(1);

                    if ( (LA55_0==K_PERMISSIONS) ) {
                        alt55=1;
                    }
                    switch (alt55) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:574:15: K_PERMISSIONS
                            {
                            match(input,K_PERMISSIONS,FOLLOW_K_PERMISSIONS_in_permissionOrAll3096); 

                            }
                            break;

                    }

                     perms = Permission.ALL_DATA; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:575:7: p= permission ( K_PERMISSION )?
                    {
                    pushFollow(FOLLOW_permission_in_permissionOrAll3117);
                    p=permission();

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:575:20: ( K_PERMISSION )?
                    int alt56=2;
                    int LA56_0 = input.LA(1);

                    if ( (LA56_0==K_PERMISSION) ) {
                        alt56=1;
                    }
                    switch (alt56) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:575:22: K_PERMISSION
                            {
                            match(input,K_PERMISSION,FOLLOW_K_PERMISSION_in_permissionOrAll3121); 

                            }
                            break;

                    }

                     perms = EnumSet.of(p); 

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
        return perms;
    }
    // $ANTLR end "permissionOrAll"


    // $ANTLR start "resource"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:578:1: resource returns [IResource res] : r= dataResource ;
    public final IResource resource() throws RecognitionException {
        IResource res = null;

        DataResource r = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:579:5: (r= dataResource )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:579:7: r= dataResource
            {
            pushFollow(FOLLOW_dataResource_in_resource3149);
            r=dataResource();

            state._fsp--;

             res = r; 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return res;
    }
    // $ANTLR end "resource"


    // $ANTLR start "dataResource"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:582:1: dataResource returns [DataResource res] : ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName );
    public final DataResource dataResource() throws RecognitionException {
        DataResource res = null;

        String ks = null;

        CFName cf = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:583:5: ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName )
            int alt59=3;
            switch ( input.LA(1) ) {
            case K_ALL:
                {
                int LA59_1 = input.LA(2);

                if ( (LA59_1==K_KEYSPACES) ) {
                    alt59=1;
                }
                else if ( (LA59_1==EOF||LA59_1==K_FROM||LA59_1==K_TO||(LA59_1>=K_OF && LA59_1<=K_NORECURSIVE)||LA59_1==125||LA59_1==132) ) {
                    alt59=3;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 59, 1, input);

                    throw nvae;
                }
                }
                break;
            case K_KEYSPACE:
                {
                alt59=2;
                }
                break;
            case K_COUNT:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_COLUMNFAMILY:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case IDENT:
            case K_TYPE:
            case K_LIST:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case QUOTED_NAME:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt59=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 59, 0, input);

                throw nvae;
            }

            switch (alt59) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:583:7: K_ALL K_KEYSPACES
                    {
                    match(input,K_ALL,FOLLOW_K_ALL_in_dataResource3172); 
                    match(input,K_KEYSPACES,FOLLOW_K_KEYSPACES_in_dataResource3174); 
                     res = DataResource.root(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:584:7: K_KEYSPACE ks= keyspaceName
                    {
                    match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dataResource3184); 
                    pushFollow(FOLLOW_keyspaceName_in_dataResource3190);
                    ks=keyspaceName();

                    state._fsp--;

                     res = DataResource.keyspace(ks); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:585:7: ( K_COLUMNFAMILY )? cf= columnFamilyName
                    {
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:585:7: ( K_COLUMNFAMILY )?
                    int alt58=2;
                    int LA58_0 = input.LA(1);

                    if ( (LA58_0==K_COLUMNFAMILY) ) {
                        alt58=1;
                    }
                    switch (alt58) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:585:9: K_COLUMNFAMILY
                            {
                            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dataResource3202); 

                            }
                            break;

                    }

                    pushFollow(FOLLOW_columnFamilyName_in_dataResource3211);
                    cf=columnFamilyName();

                    state._fsp--;

                     res = DataResource.columnFamily(cf.getKeyspace(), cf.getColumnFamily()); 

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
        return res;
    }
    // $ANTLR end "dataResource"


    // $ANTLR start "createUserStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:589:1: createUserStatement returns [CreateUserStatement stmt] : K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
    public final CreateUserStatement createUserStatement() throws RecognitionException {
        CreateUserStatement stmt = null;

        CqlParser.username_return username10 = null;



                UserOptions opts = new UserOptions();
                boolean superuser = false;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:597:5: ( K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:597:7: K_CREATE K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createUserStatement3251); 
            match(input,K_USER,FOLLOW_K_USER_in_createUserStatement3253); 
            pushFollow(FOLLOW_username_in_createUserStatement3255);
            username10=username();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:598:7: ( K_WITH userOptions[opts] )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==K_WITH) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:598:9: K_WITH userOptions[opts]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_createUserStatement3265); 
                    pushFollow(FOLLOW_userOptions_in_createUserStatement3267);
                    userOptions(opts);

                    state._fsp--;


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:599:7: ( K_SUPERUSER | K_NOSUPERUSER )?
            int alt61=3;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==K_SUPERUSER) ) {
                alt61=1;
            }
            else if ( (LA61_0==K_NOSUPERUSER) ) {
                alt61=2;
            }
            switch (alt61) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:599:9: K_SUPERUSER
                    {
                    match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_createUserStatement3281); 
                     superuser = true; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:599:45: K_NOSUPERUSER
                    {
                    match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_createUserStatement3287); 
                     superuser = false; 

                    }
                    break;

            }

             stmt = new CreateUserStatement((username10!=null?input.toString(username10.start,username10.stop):null), opts, superuser); 

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
    // $ANTLR end "createUserStatement"


    // $ANTLR start "alterUserStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:603:1: alterUserStatement returns [AlterUserStatement stmt] : K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
    public final AlterUserStatement alterUserStatement() throws RecognitionException {
        AlterUserStatement stmt = null;

        CqlParser.username_return username11 = null;



                UserOptions opts = new UserOptions();
                Boolean superuser = null;
            
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:611:5: ( K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:611:7: K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
            {
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterUserStatement3332); 
            match(input,K_USER,FOLLOW_K_USER_in_alterUserStatement3334); 
            pushFollow(FOLLOW_username_in_alterUserStatement3336);
            username11=username();

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:612:7: ( K_WITH userOptions[opts] )?
            int alt62=2;
            int LA62_0 = input.LA(1);

            if ( (LA62_0==K_WITH) ) {
                alt62=1;
            }
            switch (alt62) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:612:9: K_WITH userOptions[opts]
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_alterUserStatement3346); 
                    pushFollow(FOLLOW_userOptions_in_alterUserStatement3348);
                    userOptions(opts);

                    state._fsp--;


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:613:7: ( K_SUPERUSER | K_NOSUPERUSER )?
            int alt63=3;
            int LA63_0 = input.LA(1);

            if ( (LA63_0==K_SUPERUSER) ) {
                alt63=1;
            }
            else if ( (LA63_0==K_NOSUPERUSER) ) {
                alt63=2;
            }
            switch (alt63) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:613:9: K_SUPERUSER
                    {
                    match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_alterUserStatement3362); 
                     superuser = true; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:613:45: K_NOSUPERUSER
                    {
                    match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_alterUserStatement3368); 
                     superuser = false; 

                    }
                    break;

            }

             stmt = new AlterUserStatement((username11!=null?input.toString(username11.start,username11.stop):null), opts, superuser); 

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
    // $ANTLR end "alterUserStatement"


    // $ANTLR start "dropUserStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:617:1: dropUserStatement returns [DropUserStatement stmt] : K_DROP K_USER username ;
    public final DropUserStatement dropUserStatement() throws RecognitionException {
        DropUserStatement stmt = null;

        CqlParser.username_return username12 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:621:5: ( K_DROP K_USER username )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:621:7: K_DROP K_USER username
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropUserStatement3404); 
            match(input,K_USER,FOLLOW_K_USER_in_dropUserStatement3406); 
            pushFollow(FOLLOW_username_in_dropUserStatement3408);
            username12=username();

            state._fsp--;

             stmt = new DropUserStatement((username12!=null?input.toString(username12.start,username12.stop):null)); 

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
    // $ANTLR end "dropUserStatement"


    // $ANTLR start "listUsersStatement"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:624:1: listUsersStatement returns [ListUsersStatement stmt] : K_LIST K_USERS ;
    public final ListUsersStatement listUsersStatement() throws RecognitionException {
        ListUsersStatement stmt = null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:628:5: ( K_LIST K_USERS )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:628:7: K_LIST K_USERS
            {
            match(input,K_LIST,FOLLOW_K_LIST_in_listUsersStatement3433); 
            match(input,K_USERS,FOLLOW_K_USERS_in_listUsersStatement3435); 
             stmt = new ListUsersStatement(); 

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
    // $ANTLR end "listUsersStatement"


    // $ANTLR start "userOptions"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:631:1: userOptions[UserOptions opts] : userOption[opts] ;
    public final void userOptions(UserOptions opts) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:632:5: ( userOption[opts] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:632:7: userOption[opts]
            {
            pushFollow(FOLLOW_userOption_in_userOptions3455);
            userOption(opts);

            state._fsp--;


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
    // $ANTLR end "userOptions"


    // $ANTLR start "userOption"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:635:1: userOption[UserOptions opts] : k= K_PASSWORD v= STRING_LITERAL ;
    public final void userOption(UserOptions opts) throws RecognitionException {
        Token k=null;
        Token v=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:636:5: (k= K_PASSWORD v= STRING_LITERAL )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:636:7: k= K_PASSWORD v= STRING_LITERAL
            {
            k=(Token)match(input,K_PASSWORD,FOLLOW_K_PASSWORD_in_userOption3476); 
            v=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_userOption3480); 
             opts.put((k!=null?k.getText():null), (v!=null?v.getText():null)); 

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
    // $ANTLR end "userOption"


    // $ANTLR start "cident"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:639:1: cident returns [ColumnIdentifier id] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final ColumnIdentifier cident() throws RecognitionException {
        ColumnIdentifier id = null;

        Token t=null;
        String k = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:643:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt64=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt64=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt64=2;
                }
                break;
            case K_COUNT:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt64=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 64, 0, input);

                throw nvae;
            }

            switch (alt64) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:643:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cident3509); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:644:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cident3534); 
                     id = new ColumnIdentifier((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:645:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cident3553);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:649:1: keyspaceName returns [String id] : cfOrKsName[name, true] ;
    public final String keyspaceName() throws RecognitionException {
        String id = null;

         CFName name = new CFName(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:651:5: ( cfOrKsName[name, true] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:651:7: cfOrKsName[name, true]
            {
            pushFollow(FOLLOW_cfOrKsName_in_keyspaceName3586);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:654:1: columnFamilyName returns [CFName name] : ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] ;
    public final CFName columnFamilyName() throws RecognitionException {
        CFName name = null;

         name = new CFName(); 
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:5: ( ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false] )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:7: ( cfOrKsName[name, true] '.' )? cfOrKsName[name, false]
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:7: ( cfOrKsName[name, true] '.' )?
            int alt65=2;
            alt65 = dfa65.predict(input);
            switch (alt65) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:8: cfOrKsName[name, true] '.'
                    {
                    pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName3620);
                    cfOrKsName(name, true);

                    state._fsp--;

                    match(input,132,FOLLOW_132_in_columnFamilyName3623); 

                    }
                    break;

            }

            pushFollow(FOLLOW_cfOrKsName_in_columnFamilyName3627);
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:659:1: cfOrKsName[CFName name, boolean isKs] : (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword );
    public final void cfOrKsName(CFName name, boolean isKs) throws RecognitionException {
        Token t=null;
        String k = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:660:5: (t= IDENT | t= QUOTED_NAME | k= unreserved_keyword )
            int alt66=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt66=1;
                }
                break;
            case QUOTED_NAME:
                {
                alt66=2;
                }
                break;
            case K_COUNT:
            case K_FILTERING:
            case K_WRITETIME:
            case K_TTL:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt66=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 66, 0, input);

                throw nvae;
            }

            switch (alt66) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:660:7: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cfOrKsName3648); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), false); else name.setColumnFamily((t!=null?t.getText():null), false); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:661:7: t= QUOTED_NAME
                    {
                    t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cfOrKsName3673); 
                     if (isKs) name.setKeyspace((t!=null?t.getText():null), true); else name.setColumnFamily((t!=null?t.getText():null), true); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:662:7: k= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_cfOrKsName3692);
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


    // $ANTLR start "constant"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:665:1: constant returns [Constants.Literal constant] : (t= STRING_LITERAL | t= INTEGER | t= FLOAT | t= BOOLEAN | t= UUID | t= HEXNUMBER );
    public final Constants.Literal constant() throws RecognitionException {
        Constants.Literal constant = null;

        Token t=null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:666:5: (t= STRING_LITERAL | t= INTEGER | t= FLOAT | t= BOOLEAN | t= UUID | t= HEXNUMBER )
            int alt67=6;
            switch ( input.LA(1) ) {
            case STRING_LITERAL:
                {
                alt67=1;
                }
                break;
            case INTEGER:
                {
                alt67=2;
                }
                break;
            case FLOAT:
                {
                alt67=3;
                }
                break;
            case BOOLEAN:
                {
                alt67=4;
                }
                break;
            case UUID:
                {
                alt67=5;
                }
                break;
            case HEXNUMBER:
                {
                alt67=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 67, 0, input);

                throw nvae;
            }

            switch (alt67) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:666:7: t= STRING_LITERAL
                    {
                    t=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_constant3717); 
                     constant = Constants.Literal.string((t!=null?t.getText():null)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:667:7: t= INTEGER
                    {
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_constant3729); 
                     constant = Constants.Literal.integer((t!=null?t.getText():null)); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:668:7: t= FLOAT
                    {
                    t=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_constant3748); 
                     constant = Constants.Literal.floatingPoint((t!=null?t.getText():null)); 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:669:7: t= BOOLEAN
                    {
                    t=(Token)match(input,BOOLEAN,FOLLOW_BOOLEAN_in_constant3769); 
                     constant = Constants.Literal.bool((t!=null?t.getText():null)); 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:670:7: t= UUID
                    {
                    t=(Token)match(input,UUID,FOLLOW_UUID_in_constant3788); 
                     constant = Constants.Literal.uuid((t!=null?t.getText():null)); 

                    }
                    break;
                case 6 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:671:7: t= HEXNUMBER
                    {
                    t=(Token)match(input,HEXNUMBER,FOLLOW_HEXNUMBER_in_constant3810); 
                     constant = Constants.Literal.hex((t!=null?t.getText():null)); 

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
        return constant;
    }
    // $ANTLR end "constant"


    // $ANTLR start "set_tail"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:674:1: set_tail[List<Term.Raw> s] : ( '}' | ',' t= term set_tail[s] );
    public final void set_tail(List<Term.Raw> s) throws RecognitionException {
        Term.Raw t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:675:5: ( '}' | ',' t= term set_tail[s] )
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==133) ) {
                alt68=1;
            }
            else if ( (LA68_0==128) ) {
                alt68=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 68, 0, input);

                throw nvae;
            }
            switch (alt68) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:675:7: '}'
                    {
                    match(input,133,FOLLOW_133_in_set_tail3835); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:676:7: ',' t= term set_tail[s]
                    {
                    match(input,128,FOLLOW_128_in_set_tail3843); 
                    pushFollow(FOLLOW_term_in_set_tail3847);
                    t=term();

                    state._fsp--;

                     s.add(t); 
                    pushFollow(FOLLOW_set_tail_in_set_tail3851);
                    set_tail(s);

                    state._fsp--;


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
    // $ANTLR end "set_tail"


    // $ANTLR start "map_tail"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:679:1: map_tail[List<Pair<Term.Raw, Term.Raw>> m] : ( '}' | ',' k= term ':' v= term map_tail[m] );
    public final void map_tail(List<Pair<Term.Raw, Term.Raw>> m) throws RecognitionException {
        Term.Raw k = null;

        Term.Raw v = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:680:5: ( '}' | ',' k= term ':' v= term map_tail[m] )
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==133) ) {
                alt69=1;
            }
            else if ( (LA69_0==128) ) {
                alt69=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 69, 0, input);

                throw nvae;
            }
            switch (alt69) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:680:7: '}'
                    {
                    match(input,133,FOLLOW_133_in_map_tail3870); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:681:7: ',' k= term ':' v= term map_tail[m]
                    {
                    match(input,128,FOLLOW_128_in_map_tail3878); 
                    pushFollow(FOLLOW_term_in_map_tail3882);
                    k=term();

                    state._fsp--;

                    match(input,134,FOLLOW_134_in_map_tail3884); 
                    pushFollow(FOLLOW_term_in_map_tail3888);
                    v=term();

                    state._fsp--;

                     m.add(Pair.create(k, v)); 
                    pushFollow(FOLLOW_map_tail_in_map_tail3892);
                    map_tail(m);

                    state._fsp--;


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
    // $ANTLR end "map_tail"


    // $ANTLR start "map_literal"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:684:1: map_literal returns [Maps.Literal map] : ( '{' '}' | '{' k1= term ':' v1= term map_tail[m] );
    public final Maps.Literal map_literal() throws RecognitionException {
        Maps.Literal map = null;

        Term.Raw k1 = null;

        Term.Raw v1 = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:685:5: ( '{' '}' | '{' k1= term ':' v1= term map_tail[m] )
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==135) ) {
                int LA70_1 = input.LA(2);

                if ( (LA70_1==133) ) {
                    alt70=1;
                }
                else if ( (LA70_1==INTEGER||LA70_1==K_FILTERING||LA70_1==K_VALUES||LA70_1==K_TIMESTAMP||LA70_1==K_COUNTER||(LA70_1>=K_KEY && LA70_1<=K_CLUSTERING)||LA70_1==IDENT||LA70_1==K_TYPE||LA70_1==K_LIST||(LA70_1>=K_ALL && LA70_1<=STRING_LITERAL)||(LA70_1>=FLOAT && LA70_1<=K_TOKEN)||(LA70_1>=K_ASCII && LA70_1<=K_MAP)||LA70_1==126||LA70_1==130||LA70_1==135) ) {
                    alt70=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 70, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 70, 0, input);

                throw nvae;
            }
            switch (alt70) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:685:7: '{' '}'
                    {
                    match(input,135,FOLLOW_135_in_map_literal3914); 
                    match(input,133,FOLLOW_133_in_map_literal3916); 
                     map = new Maps.Literal(Collections.<Pair<Term.Raw, Term.Raw>>emptyList()); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:686:7: '{' k1= term ':' v1= term map_tail[m]
                    {
                    match(input,135,FOLLOW_135_in_map_literal3926); 
                     List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); 
                    pushFollow(FOLLOW_term_in_map_literal3942);
                    k1=term();

                    state._fsp--;

                    match(input,134,FOLLOW_134_in_map_literal3944); 
                    pushFollow(FOLLOW_term_in_map_literal3948);
                    v1=term();

                    state._fsp--;

                     m.add(Pair.create(k1, v1)); 
                    pushFollow(FOLLOW_map_tail_in_map_literal3952);
                    map_tail(m);

                    state._fsp--;

                     map = new Maps.Literal(m); 

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
        return map;
    }
    // $ANTLR end "map_literal"


    // $ANTLR start "set_or_map"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:691:1: set_or_map[Term.Raw t] returns [Term.Raw value] : ( ':' v= term map_tail[m] | set_tail[s] );
    public final Term.Raw set_or_map(Term.Raw t) throws RecognitionException {
        Term.Raw value = null;

        Term.Raw v = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:692:5: ( ':' v= term map_tail[m] | set_tail[s] )
            int alt71=2;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==134) ) {
                alt71=1;
            }
            else if ( (LA71_0==128||LA71_0==133) ) {
                alt71=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 71, 0, input);

                throw nvae;
            }
            switch (alt71) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:692:7: ':' v= term map_tail[m]
                    {
                    match(input,134,FOLLOW_134_in_set_or_map3984); 
                    pushFollow(FOLLOW_term_in_set_or_map3988);
                    v=term();

                    state._fsp--;

                     List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); m.add(Pair.create(t, v)); 
                    pushFollow(FOLLOW_map_tail_in_set_or_map3992);
                    map_tail(m);

                    state._fsp--;

                     value = new Maps.Literal(m); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:693:7: set_tail[s]
                    {
                     List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); 
                    pushFollow(FOLLOW_set_tail_in_set_or_map4005);
                    set_tail(s);

                    state._fsp--;

                     value = new Sets.Literal(s); 

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
        return value;
    }
    // $ANTLR end "set_or_map"


    // $ANTLR start "collection_literal"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:697:1: collection_literal returns [Term.Raw value] : ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= set_or_map[t] | '{' '}' );
    public final Term.Raw collection_literal() throws RecognitionException {
        Term.Raw value = null;

        Term.Raw t1 = null;

        Term.Raw tn = null;

        Term.Raw t = null;

        Term.Raw v = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:5: ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= set_or_map[t] | '{' '}' )
            int alt74=3;
            int LA74_0 = input.LA(1);

            if ( (LA74_0==130) ) {
                alt74=1;
            }
            else if ( (LA74_0==135) ) {
                int LA74_2 = input.LA(2);

                if ( (LA74_2==133) ) {
                    alt74=3;
                }
                else if ( (LA74_2==INTEGER||LA74_2==K_FILTERING||LA74_2==K_VALUES||LA74_2==K_TIMESTAMP||LA74_2==K_COUNTER||(LA74_2>=K_KEY && LA74_2<=K_CLUSTERING)||LA74_2==IDENT||LA74_2==K_TYPE||LA74_2==K_LIST||(LA74_2>=K_ALL && LA74_2<=STRING_LITERAL)||(LA74_2>=FLOAT && LA74_2<=K_TOKEN)||(LA74_2>=K_ASCII && LA74_2<=K_MAP)||LA74_2==126||LA74_2==130||LA74_2==135) ) {
                    alt74=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 74, 2, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 74, 0, input);

                throw nvae;
            }
            switch (alt74) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:7: '[' (t1= term ( ',' tn= term )* )? ']'
                    {
                    match(input,130,FOLLOW_130_in_collection_literal4030); 
                     List<Term.Raw> l = new ArrayList<Term.Raw>(); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:61: (t1= term ( ',' tn= term )* )?
                    int alt73=2;
                    int LA73_0 = input.LA(1);

                    if ( (LA73_0==INTEGER||LA73_0==K_FILTERING||LA73_0==K_VALUES||LA73_0==K_TIMESTAMP||LA73_0==K_COUNTER||(LA73_0>=K_KEY && LA73_0<=K_CLUSTERING)||LA73_0==IDENT||LA73_0==K_TYPE||LA73_0==K_LIST||(LA73_0>=K_ALL && LA73_0<=STRING_LITERAL)||(LA73_0>=FLOAT && LA73_0<=K_TOKEN)||(LA73_0>=K_ASCII && LA73_0<=K_MAP)||LA73_0==126||LA73_0==130||LA73_0==135) ) {
                        alt73=1;
                    }
                    switch (alt73) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:63: t1= term ( ',' tn= term )*
                            {
                            pushFollow(FOLLOW_term_in_collection_literal4038);
                            t1=term();

                            state._fsp--;

                             l.add(t1); 
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:86: ( ',' tn= term )*
                            loop72:
                            do {
                                int alt72=2;
                                int LA72_0 = input.LA(1);

                                if ( (LA72_0==128) ) {
                                    alt72=1;
                                }


                                switch (alt72) {
                            	case 1 :
                            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:88: ',' tn= term
                            	    {
                            	    match(input,128,FOLLOW_128_in_collection_literal4044); 
                            	    pushFollow(FOLLOW_term_in_collection_literal4048);
                            	    tn=term();

                            	    state._fsp--;

                            	     l.add(tn); 

                            	    }
                            	    break;

                            	default :
                            	    break loop72;
                                }
                            } while (true);


                            }
                            break;

                    }

                    match(input,131,FOLLOW_131_in_collection_literal4058); 
                     value = new Lists.Literal(l); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:699:7: '{' t= term v= set_or_map[t]
                    {
                    match(input,135,FOLLOW_135_in_collection_literal4068); 
                    pushFollow(FOLLOW_term_in_collection_literal4072);
                    t=term();

                    state._fsp--;

                    pushFollow(FOLLOW_set_or_map_in_collection_literal4076);
                    v=set_or_map(t);

                    state._fsp--;

                     value = v; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:701:7: '{' '}'
                    {
                    match(input,135,FOLLOW_135_in_collection_literal4092); 
                    match(input,133,FOLLOW_133_in_collection_literal4094); 
                     value = new Sets.Literal(Collections.<Term.Raw>emptyList()); 

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
        return value;
    }
    // $ANTLR end "collection_literal"


    // $ANTLR start "value"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:704:1: value returns [Term.Raw value] : (c= constant | l= collection_literal | QMARK );
    public final Term.Raw value() throws RecognitionException {
        Term.Raw value = null;

        Constants.Literal c = null;

        Term.Raw l = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:705:5: (c= constant | l= collection_literal | QMARK )
            int alt75=3;
            switch ( input.LA(1) ) {
            case INTEGER:
            case STRING_LITERAL:
            case FLOAT:
            case BOOLEAN:
            case UUID:
            case HEXNUMBER:
                {
                alt75=1;
                }
                break;
            case 130:
            case 135:
                {
                alt75=2;
                }
                break;
            case QMARK:
                {
                alt75=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 75, 0, input);

                throw nvae;
            }

            switch (alt75) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:705:7: c= constant
                    {
                    pushFollow(FOLLOW_constant_in_value4119);
                    c=constant();

                    state._fsp--;

                     value = c; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:706:7: l= collection_literal
                    {
                    pushFollow(FOLLOW_collection_literal_in_value4141);
                    l=collection_literal();

                    state._fsp--;

                     value = l; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:707:7: QMARK
                    {
                    match(input,QMARK,FOLLOW_QMARK_in_value4151); 
                     value = new AbstractMarker.Raw(++currentBindMarkerIdx); 

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
        return value;
    }
    // $ANTLR end "value"


    // $ANTLR start "functionName"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:710:1: functionName returns [String s] : (f= IDENT | u= unreserved_function_keyword | K_TOKEN );
    public final String functionName() throws RecognitionException {
        String s = null;

        Token f=null;
        String u = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:711:5: (f= IDENT | u= unreserved_function_keyword | K_TOKEN )
            int alt76=3;
            switch ( input.LA(1) ) {
            case IDENT:
                {
                alt76=1;
                }
                break;
            case K_FILTERING:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case K_TYPE:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt76=2;
                }
                break;
            case K_TOKEN:
                {
                alt76=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 76, 0, input);

                throw nvae;
            }

            switch (alt76) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:711:7: f= IDENT
                    {
                    f=(Token)match(input,IDENT,FOLLOW_IDENT_in_functionName4191); 
                     s = (f!=null?f.getText():null); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:712:7: u= unreserved_function_keyword
                    {
                    pushFollow(FOLLOW_unreserved_function_keyword_in_functionName4225);
                    u=unreserved_function_keyword();

                    state._fsp--;

                     s = u; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:713:7: K_TOKEN
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_functionName4235); 
                     s = "token"; 

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
    // $ANTLR end "functionName"


    // $ANTLR start "functionArgs"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:716:1: functionArgs returns [List<Term.Raw> a] : ( '(' ')' | '(' t1= term ( ',' tn= term )* ')' );
    public final List<Term.Raw> functionArgs() throws RecognitionException {
        List<Term.Raw> a = null;

        Term.Raw t1 = null;

        Term.Raw tn = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:717:5: ( '(' ')' | '(' t1= term ( ',' tn= term )* ')' )
            int alt78=2;
            int LA78_0 = input.LA(1);

            if ( (LA78_0==126) ) {
                int LA78_1 = input.LA(2);

                if ( (LA78_1==127) ) {
                    alt78=1;
                }
                else if ( (LA78_1==INTEGER||LA78_1==K_FILTERING||LA78_1==K_VALUES||LA78_1==K_TIMESTAMP||LA78_1==K_COUNTER||(LA78_1>=K_KEY && LA78_1<=K_CLUSTERING)||LA78_1==IDENT||LA78_1==K_TYPE||LA78_1==K_LIST||(LA78_1>=K_ALL && LA78_1<=STRING_LITERAL)||(LA78_1>=FLOAT && LA78_1<=K_TOKEN)||(LA78_1>=K_ASCII && LA78_1<=K_MAP)||LA78_1==126||LA78_1==130||LA78_1==135) ) {
                    alt78=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 78, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 78, 0, input);

                throw nvae;
            }
            switch (alt78) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:717:7: '(' ')'
                    {
                    match(input,126,FOLLOW_126_in_functionArgs4280); 
                    match(input,127,FOLLOW_127_in_functionArgs4282); 
                     a = Collections.emptyList(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:718:7: '(' t1= term ( ',' tn= term )* ')'
                    {
                    match(input,126,FOLLOW_126_in_functionArgs4292); 
                    pushFollow(FOLLOW_term_in_functionArgs4296);
                    t1=term();

                    state._fsp--;

                     List<Term.Raw> args = new ArrayList<Term.Raw>(); args.add(t1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:719:11: ( ',' tn= term )*
                    loop77:
                    do {
                        int alt77=2;
                        int LA77_0 = input.LA(1);

                        if ( (LA77_0==128) ) {
                            alt77=1;
                        }


                        switch (alt77) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:719:13: ',' tn= term
                    	    {
                    	    match(input,128,FOLLOW_128_in_functionArgs4312); 
                    	    pushFollow(FOLLOW_term_in_functionArgs4316);
                    	    tn=term();

                    	    state._fsp--;

                    	     args.add(tn); 

                    	    }
                    	    break;

                    	default :
                    	    break loop77;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_functionArgs4330); 
                     a = args; 

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
        return a;
    }
    // $ANTLR end "functionArgs"


    // $ANTLR start "term"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:723:1: term returns [Term.Raw term] : (v= value | f= functionName args= functionArgs | '(' c= comparatorType ')' t= term );
    public final Term.Raw term() throws RecognitionException {
        Term.Raw term = null;

        Term.Raw v = null;

        String f = null;

        List<Term.Raw> args = null;

        CQL3Type c = null;

        Term.Raw t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:724:5: (v= value | f= functionName args= functionArgs | '(' c= comparatorType ')' t= term )
            int alt79=3;
            switch ( input.LA(1) ) {
            case INTEGER:
            case STRING_LITERAL:
            case FLOAT:
            case BOOLEAN:
            case UUID:
            case HEXNUMBER:
            case QMARK:
            case 130:
            case 135:
                {
                alt79=1;
                }
                break;
            case K_FILTERING:
            case K_VALUES:
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_KEY:
            case K_COMPACT:
            case K_STORAGE:
            case K_CLUSTERING:
            case IDENT:
            case K_TYPE:
            case K_LIST:
            case K_ALL:
            case K_PERMISSIONS:
            case K_PERMISSION:
            case K_KEYSPACES:
            case K_USER:
            case K_SUPERUSER:
            case K_NOSUPERUSER:
            case K_USERS:
            case K_PASSWORD:
            case K_TOKEN:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
            case K_MAP:
                {
                alt79=2;
                }
                break;
            case 126:
                {
                alt79=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 79, 0, input);

                throw nvae;
            }

            switch (alt79) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:724:7: v= value
                    {
                    pushFollow(FOLLOW_value_in_term4355);
                    v=value();

                    state._fsp--;

                     term = v; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:725:7: f= functionName args= functionArgs
                    {
                    pushFollow(FOLLOW_functionName_in_term4392);
                    f=functionName();

                    state._fsp--;

                    pushFollow(FOLLOW_functionArgs_in_term4396);
                    args=functionArgs();

                    state._fsp--;

                     term = new FunctionCall.Raw(f, args); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:726:7: '(' c= comparatorType ')' t= term
                    {
                    match(input,126,FOLLOW_126_in_term4406); 
                    pushFollow(FOLLOW_comparatorType_in_term4410);
                    c=comparatorType();

                    state._fsp--;

                    match(input,127,FOLLOW_127_in_term4412); 
                    pushFollow(FOLLOW_term_in_term4416);
                    t=term();

                    state._fsp--;

                     term = new TypeCast(c, t); 

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


    // $ANTLR start "columnOperation"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:729:1: columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations] : (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term );
    public final void columnOperation(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations) throws RecognitionException {
        Token sig=null;
        Token i=null;
        ColumnIdentifier key = null;

        Term.Raw t = null;

        ColumnIdentifier c = null;

        Term.Raw k = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:5: (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term )
            int alt81=4;
            alt81 = dfa81.predict(input);
            switch (alt81) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:7: key= cident '=' t= term ( '+' c= cident )?
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation4439);
                    key=cident();

                    state._fsp--;

                    match(input,136,FOLLOW_136_in_columnOperation4441); 
                    pushFollow(FOLLOW_term_in_columnOperation4445);
                    t=term();

                    state._fsp--;

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:29: ( '+' c= cident )?
                    int alt80=2;
                    int LA80_0 = input.LA(1);

                    if ( (LA80_0==137) ) {
                        alt80=1;
                    }
                    switch (alt80) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:30: '+' c= cident
                            {
                            match(input,137,FOLLOW_137_in_columnOperation4448); 
                            pushFollow(FOLLOW_cident_in_columnOperation4452);
                            c=cident();

                            state._fsp--;


                            }
                            break;

                    }


                              if (c == null)
                              {
                                  addRawUpdate(operations, key, new Operation.SetValue(t));
                              }
                              else
                              {
                                  if (!key.equals(c))
                                      addRecognitionError("Only expressions of the form X = <value> + X are supported.");
                                  addRawUpdate(operations, key, new Operation.Prepend(t));
                              }
                          

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:743:7: key= cident '=' c= cident sig= ( '+' | '-' ) t= term
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation4473);
                    key=cident();

                    state._fsp--;

                    match(input,136,FOLLOW_136_in_columnOperation4475); 
                    pushFollow(FOLLOW_cident_in_columnOperation4479);
                    c=cident();

                    state._fsp--;

                    sig=(Token)input.LT(1);
                    if ( (input.LA(1)>=137 && input.LA(1)<=138) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                    pushFollow(FOLLOW_term_in_columnOperation4493);
                    t=term();

                    state._fsp--;


                              if (!key.equals(c))
                                  addRecognitionError("Only expressions of the form X = X " + (sig!=null?sig.getText():null) + "<value> are supported.");
                              addRawUpdate(operations, key, (sig!=null?sig.getText():null).equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
                          

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:749:7: key= cident '=' c= cident i= INTEGER
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation4511);
                    key=cident();

                    state._fsp--;

                    match(input,136,FOLLOW_136_in_columnOperation4513); 
                    pushFollow(FOLLOW_cident_in_columnOperation4517);
                    c=cident();

                    state._fsp--;

                    i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_columnOperation4521); 

                              // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
                              if (!key.equals(c))
                                  // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
                                  addRecognitionError("Only expressions of the form X = X " + ((i!=null?i.getText():null).charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
                              addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer((i!=null?i.getText():null))));
                          

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:757:7: key= cident '[' k= term ']' '=' t= term
                    {
                    pushFollow(FOLLOW_cident_in_columnOperation4539);
                    key=cident();

                    state._fsp--;

                    match(input,130,FOLLOW_130_in_columnOperation4541); 
                    pushFollow(FOLLOW_term_in_columnOperation4545);
                    k=term();

                    state._fsp--;

                    match(input,131,FOLLOW_131_in_columnOperation4547); 
                    match(input,136,FOLLOW_136_in_columnOperation4549); 
                    pushFollow(FOLLOW_term_in_columnOperation4553);
                    t=term();

                    state._fsp--;


                              addRawUpdate(operations, key, new Operation.SetElement(k, t));
                          

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
    // $ANTLR end "columnOperation"


    // $ANTLR start "properties"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:1: properties[PropertyDefinitions props] : property[props] ( K_AND property[props] )* ;
    public final void properties(PropertyDefinitions props) throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:764:5: ( property[props] ( K_AND property[props] )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:764:7: property[props] ( K_AND property[props] )*
            {
            pushFollow(FOLLOW_property_in_properties4579);
            property(props);

            state._fsp--;

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:764:23: ( K_AND property[props] )*
            loop82:
            do {
                int alt82=2;
                int LA82_0 = input.LA(1);

                if ( (LA82_0==K_AND) ) {
                    alt82=1;
                }


                switch (alt82) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:764:24: K_AND property[props]
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_properties4583); 
            	    pushFollow(FOLLOW_property_in_properties4585);
            	    property(props);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop82;
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
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:767:1: property[PropertyDefinitions props] : k= cident '=' (simple= propertyValue | map= map_literal ) ;
    public final void property(PropertyDefinitions props) throws RecognitionException {
        ColumnIdentifier k = null;

        String simple = null;

        Maps.Literal map = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:768:5: (k= cident '=' (simple= propertyValue | map= map_literal ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:768:7: k= cident '=' (simple= propertyValue | map= map_literal )
            {
            pushFollow(FOLLOW_cident_in_property4608);
            k=cident();

            state._fsp--;

            match(input,136,FOLLOW_136_in_property4610); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:768:20: (simple= propertyValue | map= map_literal )
            int alt83=2;
            int LA83_0 = input.LA(1);

            if ( (LA83_0==K_COUNT||LA83_0==INTEGER||(LA83_0>=K_FILTERING && LA83_0<=K_TTL)||LA83_0==K_VALUES||LA83_0==K_TIMESTAMP||LA83_0==K_COUNTER||(LA83_0>=K_KEY && LA83_0<=K_CLUSTERING)||LA83_0==K_TYPE||LA83_0==K_LIST||(LA83_0>=K_ALL && LA83_0<=STRING_LITERAL)||(LA83_0>=FLOAT && LA83_0<=HEXNUMBER)||(LA83_0>=K_ASCII && LA83_0<=K_MAP)) ) {
                alt83=1;
            }
            else if ( (LA83_0==135) ) {
                alt83=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 83, 0, input);

                throw nvae;
            }
            switch (alt83) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:768:21: simple= propertyValue
                    {
                    pushFollow(FOLLOW_propertyValue_in_property4615);
                    simple=propertyValue();

                    state._fsp--;

                     try { props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:769:24: map= map_literal
                    {
                    pushFollow(FOLLOW_map_literal_in_property4644);
                    map=map_literal();

                    state._fsp--;

                     try { props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 

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
    // $ANTLR end "property"


    // $ANTLR start "propertyValue"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:772:1: propertyValue returns [String str] : (c= constant | u= unreserved_keyword );
    public final String propertyValue() throws RecognitionException {
        String str = null;

        Constants.Literal c = null;

        String u = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:773:5: (c= constant | u= unreserved_keyword )
            int alt84=2;
            int LA84_0 = input.LA(1);

            if ( (LA84_0==INTEGER||LA84_0==STRING_LITERAL||(LA84_0>=FLOAT && LA84_0<=HEXNUMBER)) ) {
                alt84=1;
            }
            else if ( (LA84_0==K_COUNT||(LA84_0>=K_FILTERING && LA84_0<=K_TTL)||LA84_0==K_VALUES||LA84_0==K_TIMESTAMP||LA84_0==K_COUNTER||(LA84_0>=K_KEY && LA84_0<=K_CLUSTERING)||LA84_0==K_TYPE||LA84_0==K_LIST||(LA84_0>=K_ALL && LA84_0<=K_PASSWORD)||(LA84_0>=K_ASCII && LA84_0<=K_MAP)) ) {
                alt84=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 84, 0, input);

                throw nvae;
            }
            switch (alt84) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:773:7: c= constant
                    {
                    pushFollow(FOLLOW_constant_in_propertyValue4672);
                    c=constant();

                    state._fsp--;

                     str = c.getRawText(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:774:7: u= unreserved_keyword
                    {
                    pushFollow(FOLLOW_unreserved_keyword_in_propertyValue4694);
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


    // $ANTLR start "relationType"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:777:1: relationType returns [Relation.Type op] : ( '=' | '<' | '<=' | '>' | '>=' );
    public final Relation.Type relationType() throws RecognitionException {
        Relation.Type op = null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:778:5: ( '=' | '<' | '<=' | '>' | '>=' )
            int alt85=5;
            switch ( input.LA(1) ) {
            case 136:
                {
                alt85=1;
                }
                break;
            case 139:
                {
                alt85=2;
                }
                break;
            case 140:
                {
                alt85=3;
                }
                break;
            case 141:
                {
                alt85=4;
                }
                break;
            case 142:
                {
                alt85=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 85, 0, input);

                throw nvae;
            }

            switch (alt85) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:778:7: '='
                    {
                    match(input,136,FOLLOW_136_in_relationType4717); 
                     op = Relation.Type.EQ; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:779:7: '<'
                    {
                    match(input,139,FOLLOW_139_in_relationType4728); 
                     op = Relation.Type.LT; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:780:7: '<='
                    {
                    match(input,140,FOLLOW_140_in_relationType4739); 
                     op = Relation.Type.LTE; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:781:7: '>'
                    {
                    match(input,141,FOLLOW_141_in_relationType4749); 
                     op = Relation.Type.GT; 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:782:7: '>='
                    {
                    match(input,142,FOLLOW_142_in_relationType4760); 
                     op = Relation.Type.GTE; 

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
        return op;
    }
    // $ANTLR end "relationType"


    // $ANTLR start "relation"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:785:1: relation[List<Relation> clauses] : (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN '(' f1= term ( ',' fN= term )* ')' );
    public final void relation(List<Relation> clauses) throws RecognitionException {
        ColumnIdentifier name = null;

        Relation.Type type = null;

        Term.Raw t = null;

        ColumnIdentifier name1 = null;

        ColumnIdentifier namen = null;

        Term.Raw f1 = null;

        Term.Raw fN = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:786:5: (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN '(' f1= term ( ',' fN= term )* ')' )
            int alt88=3;
            alt88 = dfa88.predict(input);
            switch (alt88) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:786:7: name= cident type= relationType t= term
                    {
                    pushFollow(FOLLOW_cident_in_relation4782);
                    name=cident();

                    state._fsp--;

                    pushFollow(FOLLOW_relationType_in_relation4786);
                    type=relationType();

                    state._fsp--;

                    pushFollow(FOLLOW_term_in_relation4790);
                    t=term();

                    state._fsp--;

                     clauses.add(new Relation(name, type, t)); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:787:7: K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term
                    {
                    match(input,K_TOKEN,FOLLOW_K_TOKEN_in_relation4800); 
                     List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); 
                    match(input,126,FOLLOW_126_in_relation4823); 
                    pushFollow(FOLLOW_cident_in_relation4827);
                    name1=cident();

                    state._fsp--;

                     l.add(name1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:789:46: ( ',' namen= cident )*
                    loop86:
                    do {
                        int alt86=2;
                        int LA86_0 = input.LA(1);

                        if ( (LA86_0==128) ) {
                            alt86=1;
                        }


                        switch (alt86) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:789:48: ',' namen= cident
                    	    {
                    	    match(input,128,FOLLOW_128_in_relation4833); 
                    	    pushFollow(FOLLOW_cident_in_relation4837);
                    	    namen=cident();

                    	    state._fsp--;

                    	     l.add(namen); 

                    	    }
                    	    break;

                    	default :
                    	    break loop86;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_relation4843); 
                    pushFollow(FOLLOW_relationType_in_relation4855);
                    type=relationType();

                    state._fsp--;

                    pushFollow(FOLLOW_term_in_relation4859);
                    t=term();

                    state._fsp--;


                                for (ColumnIdentifier id : l)
                                    clauses.add(new Relation(id, type, t, true));
                            

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:7: name= cident K_IN '(' f1= term ( ',' fN= term )* ')'
                    {
                    pushFollow(FOLLOW_cident_in_relation4879);
                    name=cident();

                    state._fsp--;

                    match(input,K_IN,FOLLOW_K_IN_in_relation4881); 
                     Relation rel = Relation.createInRelation(name); 
                    match(input,126,FOLLOW_126_in_relation4892); 
                    pushFollow(FOLLOW_term_in_relation4896);
                    f1=term();

                    state._fsp--;

                     rel.addInValue(f1); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:796:44: ( ',' fN= term )*
                    loop87:
                    do {
                        int alt87=2;
                        int LA87_0 = input.LA(1);

                        if ( (LA87_0==128) ) {
                            alt87=1;
                        }


                        switch (alt87) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:796:45: ',' fN= term
                    	    {
                    	    match(input,128,FOLLOW_128_in_relation4901); 
                    	    pushFollow(FOLLOW_term_in_relation4905);
                    	    fN=term();

                    	    state._fsp--;

                    	     rel.addInValue(fN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop87;
                        }
                    } while (true);

                    match(input,127,FOLLOW_127_in_relation4912); 
                     clauses.add(rel); 

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
    // $ANTLR end "relation"


    // $ANTLR start "comparatorType"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:799:1: comparatorType returns [CQL3Type t] : (c= native_type | c= collection_type | s= STRING_LITERAL );
    public final CQL3Type comparatorType() throws RecognitionException {
        CQL3Type t = null;

        Token s=null;
        CQL3Type c = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:800:5: (c= native_type | c= collection_type | s= STRING_LITERAL )
            int alt89=3;
            switch ( input.LA(1) ) {
            case K_TIMESTAMP:
            case K_COUNTER:
            case K_ASCII:
            case K_BIGINT:
            case K_BLOB:
            case K_BOOLEAN:
            case K_DECIMAL:
            case K_DOUBLE:
            case K_FLOAT:
            case K_INET:
            case K_INT:
            case K_TEXT:
            case K_UUID:
            case K_VARCHAR:
            case K_VARINT:
            case K_TIMEUUID:
                {
                alt89=1;
                }
                break;
            case K_SET:
            case K_LIST:
            case K_MAP:
                {
                alt89=2;
                }
                break;
            case STRING_LITERAL:
                {
                alt89=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 89, 0, input);

                throw nvae;
            }

            switch (alt89) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:800:7: c= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_comparatorType4937);
                    c=native_type();

                    state._fsp--;

                     t = c; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:801:7: c= collection_type
                    {
                    pushFollow(FOLLOW_collection_type_in_comparatorType4953);
                    c=collection_type();

                    state._fsp--;

                     t = c; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:802:7: s= STRING_LITERAL
                    {
                    s=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_comparatorType4965); 

                            try {
                                t = new CQL3Type.Custom((s!=null?s.getText():null));
                            } catch (SyntaxException e) {
                                addRecognitionError("Cannot parse type " + (s!=null?s.getText():null) + ": " + e.getMessage());
                            } catch (ConfigurationException e) {
                                addRecognitionError("Error setting type " + (s!=null?s.getText():null) + ": " + e.getMessage());
                            }
                          

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
        return t;
    }
    // $ANTLR end "comparatorType"


    // $ANTLR start "native_type"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:814:1: native_type returns [CQL3Type t] : ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID );
    public final CQL3Type native_type() throws RecognitionException {
        CQL3Type t = null;

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:815:5: ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID )
            int alt90=16;
            switch ( input.LA(1) ) {
            case K_ASCII:
                {
                alt90=1;
                }
                break;
            case K_BIGINT:
                {
                alt90=2;
                }
                break;
            case K_BLOB:
                {
                alt90=3;
                }
                break;
            case K_BOOLEAN:
                {
                alt90=4;
                }
                break;
            case K_COUNTER:
                {
                alt90=5;
                }
                break;
            case K_DECIMAL:
                {
                alt90=6;
                }
                break;
            case K_DOUBLE:
                {
                alt90=7;
                }
                break;
            case K_FLOAT:
                {
                alt90=8;
                }
                break;
            case K_INET:
                {
                alt90=9;
                }
                break;
            case K_INT:
                {
                alt90=10;
                }
                break;
            case K_TEXT:
                {
                alt90=11;
                }
                break;
            case K_TIMESTAMP:
                {
                alt90=12;
                }
                break;
            case K_UUID:
                {
                alt90=13;
                }
                break;
            case K_VARCHAR:
                {
                alt90=14;
                }
                break;
            case K_VARINT:
                {
                alt90=15;
                }
                break;
            case K_TIMEUUID:
                {
                alt90=16;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 90, 0, input);

                throw nvae;
            }

            switch (alt90) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:815:7: K_ASCII
                    {
                    match(input,K_ASCII,FOLLOW_K_ASCII_in_native_type4994); 
                     t = CQL3Type.Native.ASCII; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:816:7: K_BIGINT
                    {
                    match(input,K_BIGINT,FOLLOW_K_BIGINT_in_native_type5008); 
                     t = CQL3Type.Native.BIGINT; 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:817:7: K_BLOB
                    {
                    match(input,K_BLOB,FOLLOW_K_BLOB_in_native_type5021); 
                     t = CQL3Type.Native.BLOB; 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:818:7: K_BOOLEAN
                    {
                    match(input,K_BOOLEAN,FOLLOW_K_BOOLEAN_in_native_type5036); 
                     t = CQL3Type.Native.BOOLEAN; 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:819:7: K_COUNTER
                    {
                    match(input,K_COUNTER,FOLLOW_K_COUNTER_in_native_type5048); 
                     t = CQL3Type.Native.COUNTER; 

                    }
                    break;
                case 6 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:820:7: K_DECIMAL
                    {
                    match(input,K_DECIMAL,FOLLOW_K_DECIMAL_in_native_type5060); 
                     t = CQL3Type.Native.DECIMAL; 

                    }
                    break;
                case 7 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:821:7: K_DOUBLE
                    {
                    match(input,K_DOUBLE,FOLLOW_K_DOUBLE_in_native_type5072); 
                     t = CQL3Type.Native.DOUBLE; 

                    }
                    break;
                case 8 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:822:7: K_FLOAT
                    {
                    match(input,K_FLOAT,FOLLOW_K_FLOAT_in_native_type5085); 
                     t = CQL3Type.Native.FLOAT; 

                    }
                    break;
                case 9 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:823:7: K_INET
                    {
                    match(input,K_INET,FOLLOW_K_INET_in_native_type5099); 
                     t = CQL3Type.Native.INET;

                    }
                    break;
                case 10 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:824:7: K_INT
                    {
                    match(input,K_INT,FOLLOW_K_INT_in_native_type5114); 
                     t = CQL3Type.Native.INT; 

                    }
                    break;
                case 11 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:825:7: K_TEXT
                    {
                    match(input,K_TEXT,FOLLOW_K_TEXT_in_native_type5130); 
                     t = CQL3Type.Native.TEXT; 

                    }
                    break;
                case 12 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:826:7: K_TIMESTAMP
                    {
                    match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_native_type5145); 
                     t = CQL3Type.Native.TIMESTAMP; 

                    }
                    break;
                case 13 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:827:7: K_UUID
                    {
                    match(input,K_UUID,FOLLOW_K_UUID_in_native_type5155); 
                     t = CQL3Type.Native.UUID; 

                    }
                    break;
                case 14 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:828:7: K_VARCHAR
                    {
                    match(input,K_VARCHAR,FOLLOW_K_VARCHAR_in_native_type5170); 
                     t = CQL3Type.Native.VARCHAR; 

                    }
                    break;
                case 15 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:829:7: K_VARINT
                    {
                    match(input,K_VARINT,FOLLOW_K_VARINT_in_native_type5182); 
                     t = CQL3Type.Native.VARINT; 

                    }
                    break;
                case 16 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:830:7: K_TIMEUUID
                    {
                    match(input,K_TIMEUUID,FOLLOW_K_TIMEUUID_in_native_type5195); 
                     t = CQL3Type.Native.TIMEUUID; 

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
        return t;
    }
    // $ANTLR end "native_type"


    // $ANTLR start "collection_type"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:833:1: collection_type returns [CQL3Type pt] : ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' );
    public final CQL3Type collection_type() throws RecognitionException {
        CQL3Type pt = null;

        CQL3Type t1 = null;

        CQL3Type t2 = null;

        CQL3Type t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:834:5: ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' )
            int alt91=3;
            switch ( input.LA(1) ) {
            case K_MAP:
                {
                alt91=1;
                }
                break;
            case K_LIST:
                {
                alt91=2;
                }
                break;
            case K_SET:
                {
                alt91=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 91, 0, input);

                throw nvae;
            }

            switch (alt91) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:834:7: K_MAP '<' t1= comparatorType ',' t2= comparatorType '>'
                    {
                    match(input,K_MAP,FOLLOW_K_MAP_in_collection_type5219); 
                    match(input,139,FOLLOW_139_in_collection_type5222); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type5226);
                    t1=comparatorType();

                    state._fsp--;

                    match(input,128,FOLLOW_128_in_collection_type5228); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type5232);
                    t2=comparatorType();

                    state._fsp--;

                    match(input,141,FOLLOW_141_in_collection_type5234); 
                     try {
                                // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
                                if (t1 != null && t2 != null)
                                    pt = CQL3Type.Collection.map(t1, t2);
                              } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:840:7: K_LIST '<' t= comparatorType '>'
                    {
                    match(input,K_LIST,FOLLOW_K_LIST_in_collection_type5252); 
                    match(input,139,FOLLOW_139_in_collection_type5254); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type5258);
                    t=comparatorType();

                    state._fsp--;

                    match(input,141,FOLLOW_141_in_collection_type5260); 
                     try { if (t != null) pt = CQL3Type.Collection.list(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:842:7: K_SET '<' t= comparatorType '>'
                    {
                    match(input,K_SET,FOLLOW_K_SET_in_collection_type5278); 
                    match(input,139,FOLLOW_139_in_collection_type5281); 
                    pushFollow(FOLLOW_comparatorType_in_collection_type5285);
                    t=comparatorType();

                    state._fsp--;

                    match(input,141,FOLLOW_141_in_collection_type5287); 
                     try { if (t != null) pt = CQL3Type.Collection.set(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } 

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
        return pt;
    }
    // $ANTLR end "collection_type"

    public static class username_return extends ParserRuleReturnScope {
    };

    // $ANTLR start "username"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:846:1: username : ( IDENT | STRING_LITERAL );
    public final CqlParser.username_return username() throws RecognitionException {
        CqlParser.username_return retval = new CqlParser.username_return();
        retval.start = input.LT(1);

        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:847:5: ( IDENT | STRING_LITERAL )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
            {
            if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "username"


    // $ANTLR start "unreserved_keyword"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:851:1: unreserved_keyword returns [String str] : (u= unreserved_function_keyword | k= ( K_TTL | K_COUNT | K_WRITETIME ) );
    public final String unreserved_keyword() throws RecognitionException {
        String str = null;

        Token k=null;
        String u = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:852:5: (u= unreserved_function_keyword | k= ( K_TTL | K_COUNT | K_WRITETIME ) )
            int alt92=2;
            int LA92_0 = input.LA(1);

            if ( (LA92_0==K_FILTERING||LA92_0==K_VALUES||LA92_0==K_TIMESTAMP||LA92_0==K_COUNTER||(LA92_0>=K_KEY && LA92_0<=K_CLUSTERING)||LA92_0==K_TYPE||LA92_0==K_LIST||(LA92_0>=K_ALL && LA92_0<=K_PASSWORD)||(LA92_0>=K_ASCII && LA92_0<=K_MAP)) ) {
                alt92=1;
            }
            else if ( (LA92_0==K_COUNT||(LA92_0>=K_WRITETIME && LA92_0<=K_TTL)) ) {
                alt92=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 92, 0, input);

                throw nvae;
            }
            switch (alt92) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:852:7: u= unreserved_function_keyword
                    {
                    pushFollow(FOLLOW_unreserved_function_keyword_in_unreserved_keyword5345);
                    u=unreserved_function_keyword();

                    state._fsp--;

                     str = u; 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:853:7: k= ( K_TTL | K_COUNT | K_WRITETIME )
                    {
                    k=(Token)input.LT(1);
                    if ( input.LA(1)==K_COUNT||(input.LA(1)>=K_WRITETIME && input.LA(1)<=K_TTL) ) {
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


    // $ANTLR start "unreserved_function_keyword"
    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:856:1: unreserved_function_keyword returns [String str] : (k= ( K_KEY | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD ) | t= native_type );
    public final String unreserved_function_keyword() throws RecognitionException {
        String str = null;

        Token k=null;
        CQL3Type t = null;


        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:857:5: (k= ( K_KEY | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD ) | t= native_type )
            int alt93=2;
            int LA93_0 = input.LA(1);

            if ( (LA93_0==K_FILTERING||LA93_0==K_VALUES||(LA93_0>=K_KEY && LA93_0<=K_CLUSTERING)||LA93_0==K_TYPE||LA93_0==K_LIST||(LA93_0>=K_ALL && LA93_0<=K_PASSWORD)||LA93_0==K_MAP) ) {
                alt93=1;
            }
            else if ( (LA93_0==K_TIMESTAMP||LA93_0==K_COUNTER||(LA93_0>=K_ASCII && LA93_0<=K_TIMEUUID)) ) {
                alt93=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 93, 0, input);

                throw nvae;
            }
            switch (alt93) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:857:7: k= ( K_KEY | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD )
                    {
                    k=(Token)input.LT(1);
                    if ( input.LA(1)==K_FILTERING||input.LA(1)==K_VALUES||(input.LA(1)>=K_KEY && input.LA(1)<=K_CLUSTERING)||input.LA(1)==K_TYPE||input.LA(1)==K_LIST||(input.LA(1)>=K_ALL && input.LA(1)<=K_PASSWORD)||input.LA(1)==K_MAP ) {
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
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:876:7: t= native_type
                    {
                    pushFollow(FOLLOW_native_type_in_unreserved_function_keyword5624);
                    t=native_type();

                    state._fsp--;

                     str = t.toString(); 

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
    // $ANTLR end "unreserved_function_keyword"

    // Delegated rules


    protected DFA2 dfa2 = new DFA2(this);
    protected DFA13 dfa13 = new DFA13(this);
    protected DFA30 dfa30 = new DFA30(this);
    protected DFA65 dfa65 = new DFA65(this);
    protected DFA81 dfa81 = new DFA81(this);
    protected DFA88 dfa88 = new DFA88(this);
    static final String DFA2_eotS =
        "\33\uffff";
    static final String DFA2_eofS =
        "\33\uffff";
    static final String DFA2_minS =
        "\1\4\7\uffff\3\42\2\uffff\1\5\15\uffff";
    static final String DFA2_maxS =
        "\1\66\7\uffff\3\77\2\uffff\1\102\15\uffff";
    static final String DFA2_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\3\uffff\1\20\1\21\1\uffff\1"+
        "\10\1\11\1\12\1\23\1\13\1\14\1\15\1\25\1\16\1\17\1\24\1\26\1\22";
    static final String DFA2_specialS =
        "\33\uffff}>";
    static final String[] DFA2_transitionS = {
            "\1\6\1\1\16\uffff\1\2\4\uffff\1\3\1\uffff\1\5\1\4\4\uffff\1"+
            "\10\13\uffff\1\12\4\uffff\1\11\1\7\1\13\1\14\1\15",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\16\1\uffff\1\17\5\uffff\1\20\24\uffff\1\21",
            "\1\22\1\uffff\1\23\5\uffff\1\24\24\uffff\1\25",
            "\1\27\1\uffff\1\26\32\uffff\1\30",
            "",
            "",
            "\1\32\33\uffff\1\32\13\uffff\1\32\4\uffff\1\32\6\uffff\3\32"+
            "\6\uffff\1\31",
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
            return "172:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement | st2= insertStatement | st3= updateStatement | st4= batchStatement | st5= deleteStatement | st6= useStatement | st7= truncateStatement | st8= createKeyspaceStatement | st9= createColumnFamilyStatement | st10= createIndexStatement | st11= dropKeyspaceStatement | st12= dropColumnFamilyStatement | st13= dropIndexStatement | st14= alterTableStatement | st15= alterKeyspaceStatement | st16= grantStatement | st17= revokeStatement | st18= listPermissionsStatement | st19= createUserStatement | st20= alterUserStatement | st21= dropUserStatement | st22= listUsersStatement );";
        }
    }
    static final String DFA13_eotS =
        "\31\uffff";
    static final String DFA13_eofS =
        "\31\uffff";
    static final String DFA13_minS =
        "\1\6\1\7\1\uffff\23\7\3\uffff";
    static final String DFA13_maxS =
        "\1\133\1\u0080\1\uffff\23\u0080\3\uffff";
    static final String DFA13_acceptS =
        "\2\uffff\1\1\23\uffff\1\4\1\2\1\3";
    static final String DFA13_specialS =
        "\31\uffff}>";
    static final String[] DFA13_transitionS = {
            "\1\2\7\uffff\1\3\1\24\1\25\5\uffff\1\3\1\uffff\1\17\5\uffff"+
            "\1\10\7\uffff\4\3\1\uffff\1\1\2\uffff\1\3\7\uffff\1\3\4\uffff"+
            "\11\3\1\uffff\1\2\5\uffff\1\26\1\uffff\1\4\1\5\1\6\1\7\1\11"+
            "\1\12\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\2\166\uffff\1\26\2\2",
            "",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\26\2\2",
            "\1\2\166\uffff\1\27\2\2",
            "\1\2\166\uffff\1\30\2\2",
            "",
            "",
            ""
    };

    static final short[] DFA13_eot = DFA.unpackEncodedString(DFA13_eotS);
    static final short[] DFA13_eof = DFA.unpackEncodedString(DFA13_eofS);
    static final char[] DFA13_min = DFA.unpackEncodedStringToUnsignedChars(DFA13_minS);
    static final char[] DFA13_max = DFA.unpackEncodedStringToUnsignedChars(DFA13_maxS);
    static final short[] DFA13_accept = DFA.unpackEncodedString(DFA13_acceptS);
    static final short[] DFA13_special = DFA.unpackEncodedString(DFA13_specialS);
    static final short[][] DFA13_transition;

    static {
        int numStates = DFA13_transitionS.length;
        DFA13_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA13_transition[i] = DFA.unpackEncodedString(DFA13_transitionS[i]);
        }
    }

    class DFA13 extends DFA {

        public DFA13(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 13;
            this.eot = DFA13_eot;
            this.eof = DFA13_eof;
            this.min = DFA13_min;
            this.max = DFA13_max;
            this.accept = DFA13_accept;
            this.special = DFA13_special;
            this.transition = DFA13_transition;
        }
        public String getDescription() {
            return "245:1: selector returns [RawSelector s] : (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' | f= functionName args= selectionFunctionArgs );";
        }
    }
    static final String DFA30_eotS =
        "\27\uffff";
    static final String DFA30_eofS =
        "\27\uffff";
    static final String DFA30_minS =
        "\1\6\24\7\2\uffff";
    static final String DFA30_maxS =
        "\1\133\24\u0082\2\uffff";
    static final String DFA30_acceptS =
        "\25\uffff\1\2\1\1";
    static final String DFA30_specialS =
        "\27\uffff}>";
    static final String[] DFA30_transitionS = {
            "\1\24\7\uffff\1\3\2\24\5\uffff\1\3\1\uffff\1\17\5\uffff\1\10"+
            "\7\uffff\4\3\1\uffff\1\1\2\uffff\1\3\7\uffff\1\3\4\uffff\11"+
            "\3\1\uffff\1\2\7\uffff\1\4\1\5\1\6\1\7\1\11\1\12\1\13\1\14\1"+
            "\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "\1\26\170\uffff\1\26\1\uffff\1\25",
            "",
            ""
    };

    static final short[] DFA30_eot = DFA.unpackEncodedString(DFA30_eotS);
    static final short[] DFA30_eof = DFA.unpackEncodedString(DFA30_eofS);
    static final char[] DFA30_min = DFA.unpackEncodedStringToUnsignedChars(DFA30_minS);
    static final char[] DFA30_max = DFA.unpackEncodedStringToUnsignedChars(DFA30_maxS);
    static final short[] DFA30_accept = DFA.unpackEncodedString(DFA30_acceptS);
    static final short[] DFA30_special = DFA.unpackEncodedString(DFA30_specialS);
    static final short[][] DFA30_transition;

    static {
        int numStates = DFA30_transitionS.length;
        DFA30_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA30_transition[i] = DFA.unpackEncodedString(DFA30_transitionS[i]);
        }
    }

    class DFA30 extends DFA {

        public DFA30(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 30;
            this.eot = DFA30_eot;
            this.eof = DFA30_eof;
            this.min = DFA30_min;
            this.max = DFA30_max;
            this.accept = DFA30_accept;
            this.special = DFA30_special;
            this.transition = DFA30_transition;
        }
        public String getDescription() {
            return "355:1: deleteOp returns [Operation.RawDeletion op] : (c= cident | c= cident '[' t= term ']' );";
        }
    }
    static final String DFA65_eotS =
        "\27\uffff";
    static final String DFA65_eofS =
        "\1\uffff\24\25\2\uffff";
    static final String DFA65_minS =
        "\1\6\24\7\2\uffff";
    static final String DFA65_maxS =
        "\1\133\24\u0084\2\uffff";
    static final String DFA65_acceptS =
        "\25\uffff\1\2\1\1";
    static final String DFA65_specialS =
        "\27\uffff}>";
    static final String[] DFA65_transitionS = {
            "\1\24\7\uffff\1\3\2\24\5\uffff\1\3\1\uffff\1\17\5\uffff\1\10"+
            "\7\uffff\4\3\1\uffff\1\1\2\uffff\1\3\7\uffff\1\3\4\uffff\11"+
            "\3\1\uffff\1\2\7\uffff\1\4\1\5\1\6\1\7\1\11\1\12\1\13\1\14\1"+
            "\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "\3\25\1\uffff\1\25\1\uffff\1\25\11\uffff\1\25\2\uffff\1\25"+
            "\10\uffff\1\25\11\uffff\1\25\1\uffff\3\25\5\uffff\2\25\104\uffff"+
            "\2\25\5\uffff\1\26",
            "",
            ""
    };

    static final short[] DFA65_eot = DFA.unpackEncodedString(DFA65_eotS);
    static final short[] DFA65_eof = DFA.unpackEncodedString(DFA65_eofS);
    static final char[] DFA65_min = DFA.unpackEncodedStringToUnsignedChars(DFA65_minS);
    static final char[] DFA65_max = DFA.unpackEncodedStringToUnsignedChars(DFA65_maxS);
    static final short[] DFA65_accept = DFA.unpackEncodedString(DFA65_acceptS);
    static final short[] DFA65_special = DFA.unpackEncodedString(DFA65_specialS);
    static final short[][] DFA65_transition;

    static {
        int numStates = DFA65_transitionS.length;
        DFA65_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA65_transition[i] = DFA.unpackEncodedString(DFA65_transitionS[i]);
        }
    }

    class DFA65 extends DFA {

        public DFA65(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 65;
            this.eot = DFA65_eot;
            this.eof = DFA65_eof;
            this.min = DFA65_min;
            this.max = DFA65_max;
            this.accept = DFA65_accept;
            this.special = DFA65_special;
            this.transition = DFA65_transition;
        }
        public String getDescription() {
            return "656:7: ( cfOrKsName[name, true] '.' )?";
        }
    }
    static final String DFA81_eotS =
        "\56\uffff";
    static final String DFA81_eofS =
        "\56\uffff";
    static final String DFA81_minS =
        "\1\6\24\u0082\1\6\2\uffff\24\14\2\uffff";
    static final String DFA81_maxS =
        "\1\133\24\u0088\1\u0087\2\uffff\24\u008a\2\uffff";
    static final String DFA81_acceptS =
        "\26\uffff\1\4\1\1\24\uffff\1\3\1\2";
    static final String DFA81_specialS =
        "\56\uffff}>";
    static final String[] DFA81_transitionS = {
            "\1\24\7\uffff\1\3\2\24\5\uffff\1\3\1\uffff\1\17\5\uffff\1\10"+
            "\7\uffff\4\3\1\uffff\1\1\2\uffff\1\3\7\uffff\1\3\4\uffff\11"+
            "\3\1\uffff\1\2\7\uffff\1\4\1\5\1\6\1\7\1\11\1\12\1\13\1\14\1"+
            "\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\26\5\uffff\1\25",
            "\1\53\5\uffff\1\27\1\uffff\1\31\2\53\5\uffff\1\31\1\uffff\1"+
            "\45\5\uffff\1\36\7\uffff\4\31\1\uffff\1\30\2\uffff\1\31\7\uffff"+
            "\1\31\4\uffff\11\31\1\27\1\52\6\27\1\uffff\1\32\1\33\1\34\1"+
            "\35\1\37\1\40\1\41\1\42\1\43\1\44\1\46\1\47\1\50\1\51\1\31\42"+
            "\uffff\1\27\3\uffff\1\27\4\uffff\1\27",
            "",
            "",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\161\uffff\1\27\12\uffff\2\55",
            "\1\54\174\uffff\2\55",
            "\1\54\174\uffff\2\55",
            "",
            ""
    };

    static final short[] DFA81_eot = DFA.unpackEncodedString(DFA81_eotS);
    static final short[] DFA81_eof = DFA.unpackEncodedString(DFA81_eofS);
    static final char[] DFA81_min = DFA.unpackEncodedStringToUnsignedChars(DFA81_minS);
    static final char[] DFA81_max = DFA.unpackEncodedStringToUnsignedChars(DFA81_maxS);
    static final short[] DFA81_accept = DFA.unpackEncodedString(DFA81_acceptS);
    static final short[] DFA81_special = DFA.unpackEncodedString(DFA81_specialS);
    static final short[][] DFA81_transition;

    static {
        int numStates = DFA81_transitionS.length;
        DFA81_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA81_transition[i] = DFA.unpackEncodedString(DFA81_transitionS[i]);
        }
    }

    class DFA81 extends DFA {

        public DFA81(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 81;
            this.eot = DFA81_eot;
            this.eof = DFA81_eof;
            this.min = DFA81_min;
            this.max = DFA81_max;
            this.accept = DFA81_accept;
            this.special = DFA81_special;
            this.transition = DFA81_transition;
        }
        public String getDescription() {
            return "729:1: columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations] : (key= cident '=' t= term ( '+' c= cident )? | key= cident '=' c= cident sig= ( '+' | '-' ) t= term | key= cident '=' c= cident i= INTEGER | key= cident '[' k= term ']' '=' t= term );";
        }
    }
    static final String DFA88_eotS =
        "\30\uffff";
    static final String DFA88_eofS =
        "\30\uffff";
    static final String DFA88_minS =
        "\1\6\24\114\3\uffff";
    static final String DFA88_maxS =
        "\1\133\24\u008e\3\uffff";
    static final String DFA88_acceptS =
        "\25\uffff\1\2\1\3\1\1";
    static final String DFA88_specialS =
        "\30\uffff}>";
    static final String[] DFA88_transitionS = {
            "\1\24\7\uffff\1\3\2\24\5\uffff\1\3\1\uffff\1\17\5\uffff\1\10"+
            "\7\uffff\4\3\1\uffff\1\1\2\uffff\1\3\7\uffff\1\3\4\uffff\11"+
            "\3\1\uffff\1\2\5\uffff\1\25\1\uffff\1\4\1\5\1\6\1\7\1\11\1\12"+
            "\1\13\1\14\1\15\1\16\1\20\1\21\1\22\1\23\1\3",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "\1\26\73\uffff\1\27\2\uffff\4\27",
            "",
            "",
            ""
    };

    static final short[] DFA88_eot = DFA.unpackEncodedString(DFA88_eotS);
    static final short[] DFA88_eof = DFA.unpackEncodedString(DFA88_eofS);
    static final char[] DFA88_min = DFA.unpackEncodedStringToUnsignedChars(DFA88_minS);
    static final char[] DFA88_max = DFA.unpackEncodedStringToUnsignedChars(DFA88_maxS);
    static final short[] DFA88_accept = DFA.unpackEncodedString(DFA88_acceptS);
    static final short[] DFA88_special = DFA.unpackEncodedString(DFA88_specialS);
    static final short[][] DFA88_transition;

    static {
        int numStates = DFA88_transitionS.length;
        DFA88_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA88_transition[i] = DFA.unpackEncodedString(DFA88_transitionS[i]);
        }
    }

    class DFA88 extends DFA {

        public DFA88(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 88;
            this.eot = DFA88_eot;
            this.eof = DFA88_eof;
            this.min = DFA88_min;
            this.max = DFA88_max;
            this.accept = DFA88_accept;
            this.special = DFA88_special;
            this.transition = DFA88_transition;
        }
        public String getDescription() {
            return "785:1: relation[List<Relation> clauses] : (name= cident type= relationType t= term | K_TOKEN '(' name1= cident ( ',' namen= cident )* ')' type= relationType t= term | name= cident K_IN '(' f1= term ( ',' fN= term )* ')' );";
        }
    }
 

    public static final BitSet FOLLOW_cqlStatement_in_query72 = new BitSet(new long[]{0x0000000000000000L,0x2000000000000000L});
    public static final BitSet FOLLOW_125_in_query75 = new BitSet(new long[]{0x0000000000000000L,0x2000000000000000L});
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
    public static final BitSet FOLLOW_alterKeyspaceStatement_in_cqlStatement411 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_grantStatement_in_cqlStatement428 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_revokeStatement_in_cqlStatement453 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listPermissionsStatement_in_cqlStatement477 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createUserStatement_in_cqlStatement492 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterUserStatement_in_cqlStatement512 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropUserStatement_in_cqlStatement533 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listUsersStatement_in_cqlStatement555 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USE_in_useStatement589 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_keyspaceName_in_useStatement593 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SELECT_in_selectStatement627 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL,0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_selectStatement633 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_COUNT_in_selectStatement638 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_selectStatement640 = new BitSet(new long[]{0x0000000000001000L,0x0000000000000000L,0x0000000000000002L});
    public static final BitSet FOLLOW_selectCountClause_in_selectStatement644 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_selectStatement646 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_selectStatement659 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_selectStatement663 = new BitSet(new long[]{0x0000000000002B02L});
    public static final BitSet FOLLOW_K_WHERE_in_selectStatement673 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_whereClause_in_selectStatement677 = new BitSet(new long[]{0x0000000000002A02L});
    public static final BitSet FOLLOW_K_ORDER_in_selectStatement690 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_K_BY_in_selectStatement692 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement694 = new BitSet(new long[]{0x0000000000002802L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_selectStatement699 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_orderByClause_in_selectStatement701 = new BitSet(new long[]{0x0000000000002802L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_LIMIT_in_selectStatement718 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_INTEGER_in_selectStatement722 = new BitSet(new long[]{0x0000000000002002L});
    public static final BitSet FOLLOW_K_ALLOW_in_selectStatement737 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_K_FILTERING_in_selectStatement739 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selector_in_selectClause776 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_selectClause781 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_selector_in_selectClause785 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_129_in_selectClause797 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_selectionFunctionArgs820 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_selectionFunctionArgs822 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_selectionFunctionArgs832 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_selector_in_selectionFunctionArgs836 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_selectionFunctionArgs852 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_selector_in_selectionFunctionArgs856 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_selectionFunctionArgs870 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_selector895 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WRITETIME_in_selector938 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_selector940 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_selector944 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_selector946 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_selector969 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_selector977 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_selector981 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_selector983 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionName_in_selector1008 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_selectionFunctionArgs_in_selector1012 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_129_in_selectCountClause1035 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_selectCountClause1057 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_relation_in_whereClause1093 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_K_AND_in_whereClause1097 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_relation_in_whereClause1099 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_cident_in_orderByClause1130 = new BitSet(new long[]{0x00000000000C0002L});
    public static final BitSet FOLLOW_K_ASC_in_orderByClause1135 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_orderByClause1139 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INSERT_in_insertStatement1177 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_K_INTO_in_insertStatement1179 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_insertStatement1183 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_insertStatement1195 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_insertStatement1199 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_insertStatement1206 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_insertStatement1210 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_insertStatement1217 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_K_VALUES_in_insertStatement1227 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_insertStatement1239 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_insertStatement1243 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_insertStatement1249 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_insertStatement1253 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_insertStatement1260 = new BitSet(new long[]{0x0000000000800002L});
    public static final BitSet FOLLOW_usingClause_in_insertStatement1272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USING_in_usingClause1302 = new BitSet(new long[]{0x0000000001010000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1304 = new BitSet(new long[]{0x0000000001030002L});
    public static final BitSet FOLLOW_K_AND_in_usingClause1309 = new BitSet(new long[]{0x0000000001010000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1312 = new BitSet(new long[]{0x0000000001030002L});
    public static final BitSet FOLLOW_K_USING_in_usingClauseDelete1334 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1336 = new BitSet(new long[]{0x0000000001020002L});
    public static final BitSet FOLLOW_K_AND_in_usingClauseDelete1341 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete1344 = new BitSet(new long[]{0x0000000001020002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1366 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseDeleteObjective1370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1390 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective1399 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseObjective1403 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1437 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_updateStatement1441 = new BitSet(new long[]{0x0000000004800000L});
    public static final BitSet FOLLOW_usingClause_in_updateStatement1451 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_K_SET_in_updateStatement1463 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnOperation_in_updateStatement1465 = new BitSet(new long[]{0x0000000000000100L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_updateStatement1469 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnOperation_in_updateStatement1471 = new BitSet(new long[]{0x0000000000000100L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_K_WHERE_in_updateStatement1482 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_whereClause_in_updateStatement1486 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1526 = new BitSet(new long[]{0xF8404BC04141C0C0L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_deleteSelection_in_deleteStatement1532 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_deleteStatement1545 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_deleteStatement1549 = new BitSet(new long[]{0x0000000000800100L});
    public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1559 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1571 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE82FL});
    public static final BitSet FOLLOW_whereClause_in_deleteStatement1575 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteOp_in_deleteSelection1618 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_deleteSelection1633 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_deleteOp_in_deleteSelection1637 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_cident_in_deleteOp1664 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_deleteOp1691 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_130_in_deleteOp1693 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_deleteOp1697 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_131_in_deleteOp1699 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BEGIN_in_batchStatement1733 = new BitSet(new long[]{0x00000000E0000000L});
    public static final BitSet FOLLOW_K_UNLOGGED_in_batchStatement1743 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_K_COUNTER_in_batchStatement1749 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1762 = new BitSet(new long[]{0x000000000A900000L});
    public static final BitSet FOLLOW_usingClause_in_batchStatement1766 = new BitSet(new long[]{0x000000000A900000L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1784 = new BitSet(new long[]{0x000000010A900000L,0x2000000000000000L});
    public static final BitSet FOLLOW_125_in_batchStatement1786 = new BitSet(new long[]{0x000000010A900000L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1795 = new BitSet(new long[]{0x000000010A900000L,0x2000000000000000L});
    public static final BitSet FOLLOW_125_in_batchStatement1797 = new BitSet(new long[]{0x000000010A900000L});
    public static final BitSet FOLLOW_K_APPLY_in_batchStatement1811 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1813 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective1844 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective1857 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective1870 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement1905 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1907 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_keyspaceName_in_createKeyspaceStatement1911 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement1919 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_properties_in_createKeyspaceStatement1921 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createColumnFamilyStatement1947 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1949 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_createColumnFamilyStatement1953 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_cfamDefinition_in_createColumnFamilyStatement1963 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_cfamDefinition1982 = new BitSet(new long[]{0xF8404BE04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition1984 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_cfamDefinition1989 = new BitSet(new long[]{0xF8404BE04141C040L,0x800000000FFFE02FL,0x0000000000000001L});
    public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition1991 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_cfamDefinition1998 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_K_WITH_in_cfamDefinition2008 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2010 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_K_AND_in_cfamDefinition2015 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2017 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_cident_in_cfamColumns2043 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_cfamColumns2047 = new BitSet(new long[]{0x0000002000000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2052 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns2054 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2066 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_K_KEY_in_cfamColumns2068 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_cfamColumns2070 = new BitSet(new long[]{0xF8404BC04141C040L,0x400000000FFFE02FL});
    public static final BitSet FOLLOW_pkDef_in_cfamColumns2072 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_cfamColumns2076 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_cfamColumns2080 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_cfamColumns2087 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_pkDef2107 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_pkDef2117 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_pkDef2123 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_pkDef2129 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_pkDef2133 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_pkDef2140 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_property_in_cfamProperty2160 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COMPACT_in_cfamProperty2169 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_STORAGE_in_cfamProperty2171 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CLUSTERING_in_cfamProperty2181 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_K_ORDER_in_cfamProperty2183 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_K_BY_in_cfamProperty2185 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_cfamProperty2187 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2189 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_cfamProperty2193 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2195 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_cfamProperty2200 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_cfamOrdering2228 = new BitSet(new long[]{0x00000000000C0000L});
    public static final BitSet FOLLOW_K_ASC_in_cfamOrdering2231 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESC_in_cfamOrdering2235 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement2264 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement2266 = new BitSet(new long[]{0x0000180000000000L});
    public static final BitSet FOLLOW_IDENT_in_createIndexStatement2271 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_K_ON_in_createIndexStatement2275 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_createIndexStatement2279 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_createIndexStatement2281 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_createIndexStatement2285 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_createIndexStatement2287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterKeyspaceStatement2327 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement2329 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_keyspaceName_in_alterKeyspaceStatement2333 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_K_WITH_in_alterKeyspaceStatement2343 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_properties_in_alterKeyspaceStatement2345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2381 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2383 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_alterTableStatement2387 = new BitSet(new long[]{0x0001A00800000000L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2401 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2405 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement2407 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2411 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ADD_in_alterTableStatement2427 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2433 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2437 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WITH_in_alterTableStatement2471 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_properties_in_alterTableStatement2474 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_RENAME_in_alterTableStatement2507 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2561 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTableStatement2563 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2567 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_K_AND_in_alterTableStatement2588 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2592 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_TO_in_alterTableStatement2594 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_alterTableStatement2598 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement2644 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2646 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_keyspaceName_in_dropKeyspaceStatement2650 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropColumnFamilyStatement2675 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2677 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_dropColumnFamilyStatement2681 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement2712 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement2714 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_IDENT_in_dropIndexStatement2718 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement2749 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_truncateStatement2753 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_GRANT_in_grantStatement2778 = new BitSet(new long[]{0x0E04200200000020L});
    public static final BitSet FOLLOW_permissionOrAll_in_grantStatement2790 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_K_ON_in_grantStatement2798 = new BitSet(new long[]{0xF8404BD44141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_resource_in_grantStatement2810 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_K_TO_in_grantStatement2818 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_grantStatement2830 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_REVOKE_in_revokeStatement2861 = new BitSet(new long[]{0x0E04200200000020L});
    public static final BitSet FOLLOW_permissionOrAll_in_revokeStatement2873 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_K_ON_in_revokeStatement2881 = new BitSet(new long[]{0xF8404BD44141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_resource_in_revokeStatement2893 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_K_FROM_in_revokeStatement2901 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_revokeStatement2913 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_listPermissionsStatement2951 = new BitSet(new long[]{0x0E04200200000020L});
    public static final BitSet FOLLOW_permissionOrAll_in_listPermissionsStatement2963 = new BitSet(new long[]{0x0180100000000002L});
    public static final BitSet FOLLOW_K_ON_in_listPermissionsStatement2973 = new BitSet(new long[]{0xF8404BD44141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_resource_in_listPermissionsStatement2975 = new BitSet(new long[]{0x0180000000000002L});
    public static final BitSet FOLLOW_K_OF_in_listPermissionsStatement2990 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_listPermissionsStatement2992 = new BitSet(new long[]{0x0100000000000002L});
    public static final BitSet FOLLOW_K_NORECURSIVE_in_listPermissionsStatement3007 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_permission3043 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALL_in_permissionOrAll3092 = new BitSet(new long[]{0x1000000000000002L});
    public static final BitSet FOLLOW_K_PERMISSIONS_in_permissionOrAll3096 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_permission_in_permissionOrAll3117 = new BitSet(new long[]{0x2000000000000002L});
    public static final BitSet FOLLOW_K_PERMISSION_in_permissionOrAll3121 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dataResource_in_resource3149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALL_in_dataResource3172 = new BitSet(new long[]{0x4000000000000000L});
    public static final BitSet FOLLOW_K_KEYSPACES_in_dataResource3174 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dataResource3184 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_keyspaceName_in_dataResource3190 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dataResource3202 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_columnFamilyName_in_dataResource3211 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createUserStatement3251 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_K_USER_in_createUserStatement3253 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_createUserStatement3255 = new BitSet(new long[]{0x0000000800000002L,0x0000000000000003L});
    public static final BitSet FOLLOW_K_WITH_in_createUserStatement3265 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_userOptions_in_createUserStatement3267 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000003L});
    public static final BitSet FOLLOW_K_SUPERUSER_in_createUserStatement3281 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_NOSUPERUSER_in_createUserStatement3287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterUserStatement3332 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_K_USER_in_alterUserStatement3334 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_alterUserStatement3336 = new BitSet(new long[]{0x0000000800000002L,0x0000000000000003L});
    public static final BitSet FOLLOW_K_WITH_in_alterUserStatement3346 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_userOptions_in_alterUserStatement3348 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000003L});
    public static final BitSet FOLLOW_K_SUPERUSER_in_alterUserStatement3362 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_NOSUPERUSER_in_alterUserStatement3368 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropUserStatement3404 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_K_USER_in_dropUserStatement3406 = new BitSet(new long[]{0x0000080000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_username_in_dropUserStatement3408 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_listUsersStatement3433 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_K_USERS_in_listUsersStatement3435 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_userOption_in_userOptions3455 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_PASSWORD_in_userOption3476 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_userOption3480 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cident3509 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cident3534 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cident3553 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_keyspaceName3586 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName3620 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_132_in_columnFamilyName3623 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cfOrKsName_in_columnFamilyName3627 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_cfOrKsName3648 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_NAME_in_cfOrKsName3673 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_cfOrKsName3692 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_constant3717 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_constant3729 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FLOAT_in_constant3748 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BOOLEAN_in_constant3769 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UUID_in_constant3788 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HEXNUMBER_in_constant3810 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_133_in_set_tail3835 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_128_in_set_tail3843 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_set_tail3847 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000021L});
    public static final BitSet FOLLOW_set_tail_in_set_tail3851 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_133_in_map_tail3870 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_128_in_map_tail3878 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_map_tail3882 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_134_in_map_tail3884 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_map_tail3888 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000021L});
    public static final BitSet FOLLOW_map_tail_in_map_tail3892 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_map_literal3914 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_133_in_map_literal3916 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_map_literal3926 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_map_literal3942 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_134_in_map_literal3944 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_map_literal3948 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000021L});
    public static final BitSet FOLLOW_map_tail_in_map_literal3952 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_134_in_set_or_map3984 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_set_or_map3988 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000021L});
    public static final BitSet FOLLOW_map_tail_in_set_or_map3992 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_tail_in_set_or_map4005 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_130_in_collection_literal4030 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x000000000000008CL});
    public static final BitSet FOLLOW_term_in_collection_literal4038 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000009L});
    public static final BitSet FOLLOW_128_in_collection_literal4044 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_collection_literal4048 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000009L});
    public static final BitSet FOLLOW_131_in_collection_literal4058 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_collection_literal4068 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_collection_literal4072 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000061L});
    public static final BitSet FOLLOW_set_or_map_in_collection_literal4076 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_collection_literal4092 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_133_in_collection_literal4094 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_value4119 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collection_literal_in_value4141 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QMARK_in_value4151 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_functionName4191 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_function_keyword_in_functionName4225 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TOKEN_in_functionName4235 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_functionArgs4280 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_functionArgs4282 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_functionArgs4292 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_functionArgs4296 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_functionArgs4312 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_functionArgs4316 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_functionArgs4330 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_value_in_term4355 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionName_in_term4392 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_functionArgs_in_term4396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_term4406 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_term4410 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L});
    public static final BitSet FOLLOW_127_in_term4412 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_term4416 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation4439 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_columnOperation4441 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_columnOperation4445 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_columnOperation4448 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_columnOperation4452 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation4473 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_columnOperation4475 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_columnOperation4479 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000600L});
    public static final BitSet FOLLOW_set_in_columnOperation4483 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_columnOperation4493 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation4511 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_columnOperation4513 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_columnOperation4517 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_INTEGER_in_columnOperation4521 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_columnOperation4539 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_130_in_columnOperation4541 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_columnOperation4545 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_131_in_columnOperation4547 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_columnOperation4549 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_columnOperation4553 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_property_in_properties4579 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_K_AND_in_properties4583 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_property_in_properties4585 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_cident_in_property4608 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_136_in_property4610 = new BitSet(new long[]{0xF8404BC04141D040L,0x000000000FFFE3FFL,0x0000000000000080L});
    public static final BitSet FOLLOW_propertyValue_in_property4615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_map_literal_in_property4644 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constant_in_propertyValue4672 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_keyword_in_propertyValue4694 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_136_in_relationType4717 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_139_in_relationType4728 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_140_in_relationType4739 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_141_in_relationType4749 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_142_in_relationType4760 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation4782 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000007900L});
    public static final BitSet FOLLOW_relationType_in_relation4786 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_relation4790 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TOKEN_in_relation4800 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_relation4823 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_relation4827 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_relation4833 = new BitSet(new long[]{0xF8404BC04141C040L,0x000000000FFFE02FL});
    public static final BitSet FOLLOW_cident_in_relation4837 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_relation4843 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000007900L});
    public static final BitSet FOLLOW_relationType_in_relation4855 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_relation4859 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cident_in_relation4879 = new BitSet(new long[]{0x0000000000000000L,0x0000000000001000L});
    public static final BitSet FOLLOW_K_IN_in_relation4881 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L});
    public static final BitSet FOLLOW_126_in_relation4892 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_relation4896 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_relation4901 = new BitSet(new long[]{0xF8404BC04141D040L,0x400000000FFFEFFFL,0x0000000000000084L});
    public static final BitSet FOLLOW_term_in_relation4905 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_127_in_relation4912 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_comparatorType4937 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_collection_type_in_comparatorType4953 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_comparatorType4965 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ASCII_in_native_type4994 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BIGINT_in_native_type5008 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BLOB_in_native_type5021 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BOOLEAN_in_native_type5036 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_COUNTER_in_native_type5048 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DECIMAL_in_native_type5060 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DOUBLE_in_native_type5072 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_FLOAT_in_native_type5085 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INET_in_native_type5099 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INT_in_native_type5114 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TEXT_in_native_type5130 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_native_type5145 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UUID_in_native_type5155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_VARCHAR_in_native_type5170 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_VARINT_in_native_type5182 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMEUUID_in_native_type5195 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_MAP_in_collection_type5219 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_collection_type5222 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_collection_type5226 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_128_in_collection_type5228 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_collection_type5232 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_141_in_collection_type5234 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIST_in_collection_type5252 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_collection_type5254 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_collection_type5258 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_141_in_collection_type5260 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SET_in_collection_type5278 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_139_in_collection_type5281 = new BitSet(new long[]{0xF84043C045404000L,0x000000000FFFE01FL});
    public static final BitSet FOLLOW_comparatorType_in_collection_type5285 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_141_in_collection_type5287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_username0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unreserved_function_keyword_in_unreserved_keyword5345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unreserved_keyword5361 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unreserved_function_keyword5396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_native_type_in_unreserved_function_keyword5624 = new BitSet(new long[]{0x0000000000000002L});

}