// $ANTLR 3.2 Sep 23, 2009 12:02:23 /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g 2013-02-21 02:44:18

    package org.apache.cassandra.cql3;

    import org.apache.cassandra.exceptions.SyntaxException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlLexer extends Lexer {
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
    public static final int K_TRUNCATE=51;
    public static final int COMMENT=123;
    public static final int K_ORDER=9;
    public static final int HEXNUMBER=73;
    public static final int K_ALL=59;
    public static final int K_OF=55;
    public static final int T__139=139;
    public static final int D=105;
    public static final int T__138=138;
    public static final int E=93;
    public static final int T__137=137;
    public static final int F=97;
    public static final int T__136=136;
    public static final int G=111;
    public static final int K_TYPE=46;
    public static final int K_KEYSPACE=34;
    public static final int K_COUNT=6;
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
    public static final int K_BATCH=31;
    public static final int T__135=135;
    public static final int K_UUID=87;
    public static final int K_ASCII=77;
    public static final int UUID=72;
    public static final int K_LIST=54;
    public static final int K_DELETE=27;
    public static final int K_TO=49;
    public static final int K_BY=10;
    public static final int FLOAT=70;
    public static final int K_SUPERUSER=64;
    public static final int K_FLOAT=83;
    public static final int K_VARINT=89;
    public static final int K_DOUBLE=82;
    public static final int K_SELECT=5;
    public static final int K_LIMIT=11;
    public static final int K_ALTER=45;
    public static final int K_BOOLEAN=80;
    public static final int K_SET=26;
    public static final int K_WHERE=8;
    public static final int QUOTED_NAME=69;
    public static final int MULTILINE_COMMENT=124;
    public static final int BOOLEAN=71;
    public static final int K_UNLOGGED=29;
    public static final int K_BLOB=79;
    public static final int K_INTO=21;
    public static final int HEX=120;
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

        List<Token> tokens = new ArrayList<Token>();

        public void emit(Token token)
        {
            state.token = token;
            tokens.add(token);
        }

        public Token nextToken()
        {
            super.nextToken();
            if (tokens.size() == 0)
                return Token.EOF_TOKEN;
            return tokens.remove(0);
        }

        private List<String> recognitionErrors = new ArrayList<String>();

        public void displayRecognitionError(String[] tokenNames, RecognitionException e)
        {
            String hdr = getErrorHeader(e);
            String msg = getErrorMessage(e, tokenNames);
            recognitionErrors.add(hdr + " " + msg);
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


    // delegates
    // delegators

    public CqlLexer() {;} 
    public CqlLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public CqlLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "/Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g"; }

    // $ANTLR start "T__125"
    public final void mT__125() throws RecognitionException {
        try {
            int _type = T__125;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:50:8: ( ';' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:50:10: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__125"

    // $ANTLR start "T__126"
    public final void mT__126() throws RecognitionException {
        try {
            int _type = T__126;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:51:8: ( '(' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:51:10: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__126"

    // $ANTLR start "T__127"
    public final void mT__127() throws RecognitionException {
        try {
            int _type = T__127;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:52:8: ( ')' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:52:10: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__127"

    // $ANTLR start "T__128"
    public final void mT__128() throws RecognitionException {
        try {
            int _type = T__128;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:53:8: ( ',' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:53:10: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__128"

    // $ANTLR start "T__129"
    public final void mT__129() throws RecognitionException {
        try {
            int _type = T__129;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:54:8: ( '\\*' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:54:10: '\\*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__129"

    // $ANTLR start "T__130"
    public final void mT__130() throws RecognitionException {
        try {
            int _type = T__130;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:55:8: ( '[' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:55:10: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__130"

    // $ANTLR start "T__131"
    public final void mT__131() throws RecognitionException {
        try {
            int _type = T__131;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:56:8: ( ']' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:56:10: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__131"

    // $ANTLR start "T__132"
    public final void mT__132() throws RecognitionException {
        try {
            int _type = T__132;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:57:8: ( '.' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:57:10: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__132"

    // $ANTLR start "T__133"
    public final void mT__133() throws RecognitionException {
        try {
            int _type = T__133;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:58:8: ( '}' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:58:10: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__133"

    // $ANTLR start "T__134"
    public final void mT__134() throws RecognitionException {
        try {
            int _type = T__134;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:59:8: ( ':' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:59:10: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__134"

    // $ANTLR start "T__135"
    public final void mT__135() throws RecognitionException {
        try {
            int _type = T__135;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:60:8: ( '{' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:60:10: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__135"

    // $ANTLR start "T__136"
    public final void mT__136() throws RecognitionException {
        try {
            int _type = T__136;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:61:8: ( '=' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:61:10: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__136"

    // $ANTLR start "T__137"
    public final void mT__137() throws RecognitionException {
        try {
            int _type = T__137;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:62:8: ( '+' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:62:10: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__137"

    // $ANTLR start "T__138"
    public final void mT__138() throws RecognitionException {
        try {
            int _type = T__138;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:63:8: ( '-' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:63:10: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__138"

    // $ANTLR start "T__139"
    public final void mT__139() throws RecognitionException {
        try {
            int _type = T__139;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:64:8: ( '<' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:64:10: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__139"

    // $ANTLR start "T__140"
    public final void mT__140() throws RecognitionException {
        try {
            int _type = T__140;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:65:8: ( '<=' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:65:10: '<='
            {
            match("<="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__140"

    // $ANTLR start "T__141"
    public final void mT__141() throws RecognitionException {
        try {
            int _type = T__141;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:66:8: ( '>' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:66:10: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__141"

    // $ANTLR start "T__142"
    public final void mT__142() throws RecognitionException {
        try {
            int _type = T__142;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:67:8: ( '>=' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:67:10: '>='
            {
            match(">="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__142"

    // $ANTLR start "K_SELECT"
    public final void mK_SELECT() throws RecognitionException {
        try {
            int _type = K_SELECT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:881:9: ( S E L E C T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:881:16: S E L E C T
            {
            mS(); 
            mE(); 
            mL(); 
            mE(); 
            mC(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_SELECT"

    // $ANTLR start "K_FROM"
    public final void mK_FROM() throws RecognitionException {
        try {
            int _type = K_FROM;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:882:7: ( F R O M )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:882:16: F R O M
            {
            mF(); 
            mR(); 
            mO(); 
            mM(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_FROM"

    // $ANTLR start "K_WHERE"
    public final void mK_WHERE() throws RecognitionException {
        try {
            int _type = K_WHERE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:883:8: ( W H E R E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:883:16: W H E R E
            {
            mW(); 
            mH(); 
            mE(); 
            mR(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_WHERE"

    // $ANTLR start "K_AND"
    public final void mK_AND() throws RecognitionException {
        try {
            int _type = K_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:884:6: ( A N D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:884:16: A N D
            {
            mA(); 
            mN(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_AND"

    // $ANTLR start "K_KEY"
    public final void mK_KEY() throws RecognitionException {
        try {
            int _type = K_KEY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:885:6: ( K E Y )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:885:16: K E Y
            {
            mK(); 
            mE(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_KEY"

    // $ANTLR start "K_INSERT"
    public final void mK_INSERT() throws RecognitionException {
        try {
            int _type = K_INSERT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:886:9: ( I N S E R T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:886:16: I N S E R T
            {
            mI(); 
            mN(); 
            mS(); 
            mE(); 
            mR(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_INSERT"

    // $ANTLR start "K_UPDATE"
    public final void mK_UPDATE() throws RecognitionException {
        try {
            int _type = K_UPDATE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:887:9: ( U P D A T E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:887:16: U P D A T E
            {
            mU(); 
            mP(); 
            mD(); 
            mA(); 
            mT(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_UPDATE"

    // $ANTLR start "K_WITH"
    public final void mK_WITH() throws RecognitionException {
        try {
            int _type = K_WITH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:888:7: ( W I T H )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:888:16: W I T H
            {
            mW(); 
            mI(); 
            mT(); 
            mH(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_WITH"

    // $ANTLR start "K_LIMIT"
    public final void mK_LIMIT() throws RecognitionException {
        try {
            int _type = K_LIMIT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:889:8: ( L I M I T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:889:16: L I M I T
            {
            mL(); 
            mI(); 
            mM(); 
            mI(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_LIMIT"

    // $ANTLR start "K_USING"
    public final void mK_USING() throws RecognitionException {
        try {
            int _type = K_USING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:890:8: ( U S I N G )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:890:16: U S I N G
            {
            mU(); 
            mS(); 
            mI(); 
            mN(); 
            mG(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_USING"

    // $ANTLR start "K_USE"
    public final void mK_USE() throws RecognitionException {
        try {
            int _type = K_USE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:891:6: ( U S E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:891:16: U S E
            {
            mU(); 
            mS(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_USE"

    // $ANTLR start "K_COUNT"
    public final void mK_COUNT() throws RecognitionException {
        try {
            int _type = K_COUNT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:892:8: ( C O U N T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:892:16: C O U N T
            {
            mC(); 
            mO(); 
            mU(); 
            mN(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_COUNT"

    // $ANTLR start "K_SET"
    public final void mK_SET() throws RecognitionException {
        try {
            int _type = K_SET;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:893:6: ( S E T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:893:16: S E T
            {
            mS(); 
            mE(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_SET"

    // $ANTLR start "K_BEGIN"
    public final void mK_BEGIN() throws RecognitionException {
        try {
            int _type = K_BEGIN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:894:8: ( B E G I N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:894:16: B E G I N
            {
            mB(); 
            mE(); 
            mG(); 
            mI(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BEGIN"

    // $ANTLR start "K_UNLOGGED"
    public final void mK_UNLOGGED() throws RecognitionException {
        try {
            int _type = K_UNLOGGED;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:895:11: ( U N L O G G E D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:895:16: U N L O G G E D
            {
            mU(); 
            mN(); 
            mL(); 
            mO(); 
            mG(); 
            mG(); 
            mE(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_UNLOGGED"

    // $ANTLR start "K_BATCH"
    public final void mK_BATCH() throws RecognitionException {
        try {
            int _type = K_BATCH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:896:8: ( B A T C H )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:896:16: B A T C H
            {
            mB(); 
            mA(); 
            mT(); 
            mC(); 
            mH(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BATCH"

    // $ANTLR start "K_APPLY"
    public final void mK_APPLY() throws RecognitionException {
        try {
            int _type = K_APPLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:897:8: ( A P P L Y )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:897:16: A P P L Y
            {
            mA(); 
            mP(); 
            mP(); 
            mL(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_APPLY"

    // $ANTLR start "K_TRUNCATE"
    public final void mK_TRUNCATE() throws RecognitionException {
        try {
            int _type = K_TRUNCATE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:898:11: ( T R U N C A T E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:898:16: T R U N C A T E
            {
            mT(); 
            mR(); 
            mU(); 
            mN(); 
            mC(); 
            mA(); 
            mT(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TRUNCATE"

    // $ANTLR start "K_DELETE"
    public final void mK_DELETE() throws RecognitionException {
        try {
            int _type = K_DELETE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:899:9: ( D E L E T E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:899:16: D E L E T E
            {
            mD(); 
            mE(); 
            mL(); 
            mE(); 
            mT(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DELETE"

    // $ANTLR start "K_IN"
    public final void mK_IN() throws RecognitionException {
        try {
            int _type = K_IN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:900:5: ( I N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:900:16: I N
            {
            mI(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_IN"

    // $ANTLR start "K_CREATE"
    public final void mK_CREATE() throws RecognitionException {
        try {
            int _type = K_CREATE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:901:9: ( C R E A T E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:901:16: C R E A T E
            {
            mC(); 
            mR(); 
            mE(); 
            mA(); 
            mT(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_CREATE"

    // $ANTLR start "K_KEYSPACE"
    public final void mK_KEYSPACE() throws RecognitionException {
        try {
            int _type = K_KEYSPACE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:902:11: ( ( K E Y S P A C E | S C H E M A ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:902:16: ( K E Y S P A C E | S C H E M A )
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:902:16: ( K E Y S P A C E | S C H E M A )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0=='K'||LA1_0=='k') ) {
                alt1=1;
            }
            else if ( (LA1_0=='S'||LA1_0=='s') ) {
                alt1=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }
            switch (alt1) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:902:18: K E Y S P A C E
                    {
                    mK(); 
                    mE(); 
                    mY(); 
                    mS(); 
                    mP(); 
                    mA(); 
                    mC(); 
                    mE(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:903:20: S C H E M A
                    {
                    mS(); 
                    mC(); 
                    mH(); 
                    mE(); 
                    mM(); 
                    mA(); 

                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_KEYSPACE"

    // $ANTLR start "K_KEYSPACES"
    public final void mK_KEYSPACES() throws RecognitionException {
        try {
            int _type = K_KEYSPACES;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:904:12: ( K E Y S P A C E S )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:904:16: K E Y S P A C E S
            {
            mK(); 
            mE(); 
            mY(); 
            mS(); 
            mP(); 
            mA(); 
            mC(); 
            mE(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_KEYSPACES"

    // $ANTLR start "K_COLUMNFAMILY"
    public final void mK_COLUMNFAMILY() throws RecognitionException {
        try {
            int _type = K_COLUMNFAMILY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:905:15: ( ( C O L U M N F A M I L Y | T A B L E ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:905:16: ( C O L U M N F A M I L Y | T A B L E )
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:905:16: ( C O L U M N F A M I L Y | T A B L E )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='C'||LA2_0=='c') ) {
                alt2=1;
            }
            else if ( (LA2_0=='T'||LA2_0=='t') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:905:18: C O L U M N F A M I L Y
                    {
                    mC(); 
                    mO(); 
                    mL(); 
                    mU(); 
                    mM(); 
                    mN(); 
                    mF(); 
                    mA(); 
                    mM(); 
                    mI(); 
                    mL(); 
                    mY(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:906:20: T A B L E
                    {
                    mT(); 
                    mA(); 
                    mB(); 
                    mL(); 
                    mE(); 

                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_COLUMNFAMILY"

    // $ANTLR start "K_INDEX"
    public final void mK_INDEX() throws RecognitionException {
        try {
            int _type = K_INDEX;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:907:8: ( I N D E X )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:907:16: I N D E X
            {
            mI(); 
            mN(); 
            mD(); 
            mE(); 
            mX(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_INDEX"

    // $ANTLR start "K_ON"
    public final void mK_ON() throws RecognitionException {
        try {
            int _type = K_ON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:908:5: ( O N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:908:16: O N
            {
            mO(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ON"

    // $ANTLR start "K_TO"
    public final void mK_TO() throws RecognitionException {
        try {
            int _type = K_TO;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:909:5: ( T O )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:909:16: T O
            {
            mT(); 
            mO(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TO"

    // $ANTLR start "K_DROP"
    public final void mK_DROP() throws RecognitionException {
        try {
            int _type = K_DROP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:910:7: ( D R O P )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:910:16: D R O P
            {
            mD(); 
            mR(); 
            mO(); 
            mP(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DROP"

    // $ANTLR start "K_PRIMARY"
    public final void mK_PRIMARY() throws RecognitionException {
        try {
            int _type = K_PRIMARY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:911:10: ( P R I M A R Y )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:911:16: P R I M A R Y
            {
            mP(); 
            mR(); 
            mI(); 
            mM(); 
            mA(); 
            mR(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_PRIMARY"

    // $ANTLR start "K_INTO"
    public final void mK_INTO() throws RecognitionException {
        try {
            int _type = K_INTO;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:912:7: ( I N T O )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:912:16: I N T O
            {
            mI(); 
            mN(); 
            mT(); 
            mO(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_INTO"

    // $ANTLR start "K_VALUES"
    public final void mK_VALUES() throws RecognitionException {
        try {
            int _type = K_VALUES;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:913:9: ( V A L U E S )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:913:16: V A L U E S
            {
            mV(); 
            mA(); 
            mL(); 
            mU(); 
            mE(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_VALUES"

    // $ANTLR start "K_TIMESTAMP"
    public final void mK_TIMESTAMP() throws RecognitionException {
        try {
            int _type = K_TIMESTAMP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:914:12: ( T I M E S T A M P )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:914:16: T I M E S T A M P
            {
            mT(); 
            mI(); 
            mM(); 
            mE(); 
            mS(); 
            mT(); 
            mA(); 
            mM(); 
            mP(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TIMESTAMP"

    // $ANTLR start "K_TTL"
    public final void mK_TTL() throws RecognitionException {
        try {
            int _type = K_TTL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:915:6: ( T T L )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:915:16: T T L
            {
            mT(); 
            mT(); 
            mL(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TTL"

    // $ANTLR start "K_ALTER"
    public final void mK_ALTER() throws RecognitionException {
        try {
            int _type = K_ALTER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:916:8: ( A L T E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:916:16: A L T E R
            {
            mA(); 
            mL(); 
            mT(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ALTER"

    // $ANTLR start "K_RENAME"
    public final void mK_RENAME() throws RecognitionException {
        try {
            int _type = K_RENAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:917:9: ( R E N A M E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:917:16: R E N A M E
            {
            mR(); 
            mE(); 
            mN(); 
            mA(); 
            mM(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_RENAME"

    // $ANTLR start "K_ADD"
    public final void mK_ADD() throws RecognitionException {
        try {
            int _type = K_ADD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:918:6: ( A D D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:918:16: A D D
            {
            mA(); 
            mD(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ADD"

    // $ANTLR start "K_TYPE"
    public final void mK_TYPE() throws RecognitionException {
        try {
            int _type = K_TYPE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:919:7: ( T Y P E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:919:16: T Y P E
            {
            mT(); 
            mY(); 
            mP(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TYPE"

    // $ANTLR start "K_COMPACT"
    public final void mK_COMPACT() throws RecognitionException {
        try {
            int _type = K_COMPACT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:920:10: ( C O M P A C T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:920:16: C O M P A C T
            {
            mC(); 
            mO(); 
            mM(); 
            mP(); 
            mA(); 
            mC(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_COMPACT"

    // $ANTLR start "K_STORAGE"
    public final void mK_STORAGE() throws RecognitionException {
        try {
            int _type = K_STORAGE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:921:10: ( S T O R A G E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:921:16: S T O R A G E
            {
            mS(); 
            mT(); 
            mO(); 
            mR(); 
            mA(); 
            mG(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_STORAGE"

    // $ANTLR start "K_ORDER"
    public final void mK_ORDER() throws RecognitionException {
        try {
            int _type = K_ORDER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:922:8: ( O R D E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:922:16: O R D E R
            {
            mO(); 
            mR(); 
            mD(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ORDER"

    // $ANTLR start "K_BY"
    public final void mK_BY() throws RecognitionException {
        try {
            int _type = K_BY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:923:5: ( B Y )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:923:16: B Y
            {
            mB(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BY"

    // $ANTLR start "K_ASC"
    public final void mK_ASC() throws RecognitionException {
        try {
            int _type = K_ASC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:924:6: ( A S C )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:924:16: A S C
            {
            mA(); 
            mS(); 
            mC(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ASC"

    // $ANTLR start "K_DESC"
    public final void mK_DESC() throws RecognitionException {
        try {
            int _type = K_DESC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:925:7: ( D E S C )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:925:16: D E S C
            {
            mD(); 
            mE(); 
            mS(); 
            mC(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DESC"

    // $ANTLR start "K_ALLOW"
    public final void mK_ALLOW() throws RecognitionException {
        try {
            int _type = K_ALLOW;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:926:8: ( A L L O W )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:926:16: A L L O W
            {
            mA(); 
            mL(); 
            mL(); 
            mO(); 
            mW(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ALLOW"

    // $ANTLR start "K_FILTERING"
    public final void mK_FILTERING() throws RecognitionException {
        try {
            int _type = K_FILTERING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:927:12: ( F I L T E R I N G )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:927:16: F I L T E R I N G
            {
            mF(); 
            mI(); 
            mL(); 
            mT(); 
            mE(); 
            mR(); 
            mI(); 
            mN(); 
            mG(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_FILTERING"

    // $ANTLR start "K_GRANT"
    public final void mK_GRANT() throws RecognitionException {
        try {
            int _type = K_GRANT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:929:8: ( G R A N T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:929:16: G R A N T
            {
            mG(); 
            mR(); 
            mA(); 
            mN(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_GRANT"

    // $ANTLR start "K_ALL"
    public final void mK_ALL() throws RecognitionException {
        try {
            int _type = K_ALL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:930:6: ( A L L )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:930:16: A L L
            {
            mA(); 
            mL(); 
            mL(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ALL"

    // $ANTLR start "K_PERMISSION"
    public final void mK_PERMISSION() throws RecognitionException {
        try {
            int _type = K_PERMISSION;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:931:13: ( P E R M I S S I O N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:931:16: P E R M I S S I O N
            {
            mP(); 
            mE(); 
            mR(); 
            mM(); 
            mI(); 
            mS(); 
            mS(); 
            mI(); 
            mO(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_PERMISSION"

    // $ANTLR start "K_PERMISSIONS"
    public final void mK_PERMISSIONS() throws RecognitionException {
        try {
            int _type = K_PERMISSIONS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:932:14: ( P E R M I S S I O N S )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:932:16: P E R M I S S I O N S
            {
            mP(); 
            mE(); 
            mR(); 
            mM(); 
            mI(); 
            mS(); 
            mS(); 
            mI(); 
            mO(); 
            mN(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_PERMISSIONS"

    // $ANTLR start "K_OF"
    public final void mK_OF() throws RecognitionException {
        try {
            int _type = K_OF;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:933:5: ( O F )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:933:16: O F
            {
            mO(); 
            mF(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_OF"

    // $ANTLR start "K_REVOKE"
    public final void mK_REVOKE() throws RecognitionException {
        try {
            int _type = K_REVOKE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:934:9: ( R E V O K E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:934:16: R E V O K E
            {
            mR(); 
            mE(); 
            mV(); 
            mO(); 
            mK(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_REVOKE"

    // $ANTLR start "K_MODIFY"
    public final void mK_MODIFY() throws RecognitionException {
        try {
            int _type = K_MODIFY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:935:9: ( M O D I F Y )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:935:16: M O D I F Y
            {
            mM(); 
            mO(); 
            mD(); 
            mI(); 
            mF(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_MODIFY"

    // $ANTLR start "K_AUTHORIZE"
    public final void mK_AUTHORIZE() throws RecognitionException {
        try {
            int _type = K_AUTHORIZE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:936:12: ( A U T H O R I Z E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:936:16: A U T H O R I Z E
            {
            mA(); 
            mU(); 
            mT(); 
            mH(); 
            mO(); 
            mR(); 
            mI(); 
            mZ(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_AUTHORIZE"

    // $ANTLR start "K_NORECURSIVE"
    public final void mK_NORECURSIVE() throws RecognitionException {
        try {
            int _type = K_NORECURSIVE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:937:14: ( N O R E C U R S I V E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:937:16: N O R E C U R S I V E
            {
            mN(); 
            mO(); 
            mR(); 
            mE(); 
            mC(); 
            mU(); 
            mR(); 
            mS(); 
            mI(); 
            mV(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_NORECURSIVE"

    // $ANTLR start "K_USER"
    public final void mK_USER() throws RecognitionException {
        try {
            int _type = K_USER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:939:7: ( U S E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:939:16: U S E R
            {
            mU(); 
            mS(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_USER"

    // $ANTLR start "K_USERS"
    public final void mK_USERS() throws RecognitionException {
        try {
            int _type = K_USERS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:940:8: ( U S E R S )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:940:16: U S E R S
            {
            mU(); 
            mS(); 
            mE(); 
            mR(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_USERS"

    // $ANTLR start "K_SUPERUSER"
    public final void mK_SUPERUSER() throws RecognitionException {
        try {
            int _type = K_SUPERUSER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:941:12: ( S U P E R U S E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:941:16: S U P E R U S E R
            {
            mS(); 
            mU(); 
            mP(); 
            mE(); 
            mR(); 
            mU(); 
            mS(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_SUPERUSER"

    // $ANTLR start "K_NOSUPERUSER"
    public final void mK_NOSUPERUSER() throws RecognitionException {
        try {
            int _type = K_NOSUPERUSER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:942:14: ( N O S U P E R U S E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:942:16: N O S U P E R U S E R
            {
            mN(); 
            mO(); 
            mS(); 
            mU(); 
            mP(); 
            mE(); 
            mR(); 
            mU(); 
            mS(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_NOSUPERUSER"

    // $ANTLR start "K_PASSWORD"
    public final void mK_PASSWORD() throws RecognitionException {
        try {
            int _type = K_PASSWORD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:943:11: ( P A S S W O R D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:943:16: P A S S W O R D
            {
            mP(); 
            mA(); 
            mS(); 
            mS(); 
            mW(); 
            mO(); 
            mR(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_PASSWORD"

    // $ANTLR start "K_CLUSTERING"
    public final void mK_CLUSTERING() throws RecognitionException {
        try {
            int _type = K_CLUSTERING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:945:13: ( C L U S T E R I N G )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:945:16: C L U S T E R I N G
            {
            mC(); 
            mL(); 
            mU(); 
            mS(); 
            mT(); 
            mE(); 
            mR(); 
            mI(); 
            mN(); 
            mG(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_CLUSTERING"

    // $ANTLR start "K_ASCII"
    public final void mK_ASCII() throws RecognitionException {
        try {
            int _type = K_ASCII;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:946:8: ( A S C I I )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:946:16: A S C I I
            {
            mA(); 
            mS(); 
            mC(); 
            mI(); 
            mI(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_ASCII"

    // $ANTLR start "K_BIGINT"
    public final void mK_BIGINT() throws RecognitionException {
        try {
            int _type = K_BIGINT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:947:9: ( B I G I N T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:947:16: B I G I N T
            {
            mB(); 
            mI(); 
            mG(); 
            mI(); 
            mN(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BIGINT"

    // $ANTLR start "K_BLOB"
    public final void mK_BLOB() throws RecognitionException {
        try {
            int _type = K_BLOB;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:948:7: ( B L O B )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:948:16: B L O B
            {
            mB(); 
            mL(); 
            mO(); 
            mB(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BLOB"

    // $ANTLR start "K_BOOLEAN"
    public final void mK_BOOLEAN() throws RecognitionException {
        try {
            int _type = K_BOOLEAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:949:10: ( B O O L E A N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:949:16: B O O L E A N
            {
            mB(); 
            mO(); 
            mO(); 
            mL(); 
            mE(); 
            mA(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_BOOLEAN"

    // $ANTLR start "K_COUNTER"
    public final void mK_COUNTER() throws RecognitionException {
        try {
            int _type = K_COUNTER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:950:10: ( C O U N T E R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:950:16: C O U N T E R
            {
            mC(); 
            mO(); 
            mU(); 
            mN(); 
            mT(); 
            mE(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_COUNTER"

    // $ANTLR start "K_DECIMAL"
    public final void mK_DECIMAL() throws RecognitionException {
        try {
            int _type = K_DECIMAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:951:10: ( D E C I M A L )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:951:16: D E C I M A L
            {
            mD(); 
            mE(); 
            mC(); 
            mI(); 
            mM(); 
            mA(); 
            mL(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DECIMAL"

    // $ANTLR start "K_DOUBLE"
    public final void mK_DOUBLE() throws RecognitionException {
        try {
            int _type = K_DOUBLE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:952:9: ( D O U B L E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:952:16: D O U B L E
            {
            mD(); 
            mO(); 
            mU(); 
            mB(); 
            mL(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DOUBLE"

    // $ANTLR start "K_FLOAT"
    public final void mK_FLOAT() throws RecognitionException {
        try {
            int _type = K_FLOAT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:953:8: ( F L O A T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:953:16: F L O A T
            {
            mF(); 
            mL(); 
            mO(); 
            mA(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_FLOAT"

    // $ANTLR start "K_INET"
    public final void mK_INET() throws RecognitionException {
        try {
            int _type = K_INET;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:954:7: ( I N E T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:954:16: I N E T
            {
            mI(); 
            mN(); 
            mE(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_INET"

    // $ANTLR start "K_INT"
    public final void mK_INT() throws RecognitionException {
        try {
            int _type = K_INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:955:6: ( I N T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:955:16: I N T
            {
            mI(); 
            mN(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_INT"

    // $ANTLR start "K_TEXT"
    public final void mK_TEXT() throws RecognitionException {
        try {
            int _type = K_TEXT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:956:7: ( T E X T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:956:16: T E X T
            {
            mT(); 
            mE(); 
            mX(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TEXT"

    // $ANTLR start "K_UUID"
    public final void mK_UUID() throws RecognitionException {
        try {
            int _type = K_UUID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:957:7: ( U U I D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:957:16: U U I D
            {
            mU(); 
            mU(); 
            mI(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_UUID"

    // $ANTLR start "K_VARCHAR"
    public final void mK_VARCHAR() throws RecognitionException {
        try {
            int _type = K_VARCHAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:958:10: ( V A R C H A R )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:958:16: V A R C H A R
            {
            mV(); 
            mA(); 
            mR(); 
            mC(); 
            mH(); 
            mA(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_VARCHAR"

    // $ANTLR start "K_VARINT"
    public final void mK_VARINT() throws RecognitionException {
        try {
            int _type = K_VARINT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:959:9: ( V A R I N T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:959:16: V A R I N T
            {
            mV(); 
            mA(); 
            mR(); 
            mI(); 
            mN(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_VARINT"

    // $ANTLR start "K_TIMEUUID"
    public final void mK_TIMEUUID() throws RecognitionException {
        try {
            int _type = K_TIMEUUID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:960:11: ( T I M E U U I D )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:960:16: T I M E U U I D
            {
            mT(); 
            mI(); 
            mM(); 
            mE(); 
            mU(); 
            mU(); 
            mI(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TIMEUUID"

    // $ANTLR start "K_TOKEN"
    public final void mK_TOKEN() throws RecognitionException {
        try {
            int _type = K_TOKEN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:961:8: ( T O K E N )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:961:16: T O K E N
            {
            mT(); 
            mO(); 
            mK(); 
            mE(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_TOKEN"

    // $ANTLR start "K_WRITETIME"
    public final void mK_WRITETIME() throws RecognitionException {
        try {
            int _type = K_WRITETIME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:962:12: ( W R I T E T I M E )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:962:16: W R I T E T I M E
            {
            mW(); 
            mR(); 
            mI(); 
            mT(); 
            mE(); 
            mT(); 
            mI(); 
            mM(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_WRITETIME"

    // $ANTLR start "K_MAP"
    public final void mK_MAP() throws RecognitionException {
        try {
            int _type = K_MAP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:964:6: ( M A P )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:964:16: M A P
            {
            mM(); 
            mA(); 
            mP(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_MAP"

    // $ANTLR start "K_LIST"
    public final void mK_LIST() throws RecognitionException {
        try {
            int _type = K_LIST;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:965:7: ( L I S T )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:965:16: L I S T
            {
            mL(); 
            mI(); 
            mS(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_LIST"

    // $ANTLR start "A"
    public final void mA() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:968:11: ( ( 'a' | 'A' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:968:13: ( 'a' | 'A' )
            {
            if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "A"

    // $ANTLR start "B"
    public final void mB() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:969:11: ( ( 'b' | 'B' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:969:13: ( 'b' | 'B' )
            {
            if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "B"

    // $ANTLR start "C"
    public final void mC() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:970:11: ( ( 'c' | 'C' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:970:13: ( 'c' | 'C' )
            {
            if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "C"

    // $ANTLR start "D"
    public final void mD() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:971:11: ( ( 'd' | 'D' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:971:13: ( 'd' | 'D' )
            {
            if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "D"

    // $ANTLR start "E"
    public final void mE() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:972:11: ( ( 'e' | 'E' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:972:13: ( 'e' | 'E' )
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "E"

    // $ANTLR start "F"
    public final void mF() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:973:11: ( ( 'f' | 'F' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:973:13: ( 'f' | 'F' )
            {
            if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "F"

    // $ANTLR start "G"
    public final void mG() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:974:11: ( ( 'g' | 'G' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:974:13: ( 'g' | 'G' )
            {
            if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "G"

    // $ANTLR start "H"
    public final void mH() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:975:11: ( ( 'h' | 'H' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:975:13: ( 'h' | 'H' )
            {
            if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "H"

    // $ANTLR start "I"
    public final void mI() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:976:11: ( ( 'i' | 'I' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:976:13: ( 'i' | 'I' )
            {
            if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "I"

    // $ANTLR start "J"
    public final void mJ() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:977:11: ( ( 'j' | 'J' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:977:13: ( 'j' | 'J' )
            {
            if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "J"

    // $ANTLR start "K"
    public final void mK() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:978:11: ( ( 'k' | 'K' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:978:13: ( 'k' | 'K' )
            {
            if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "K"

    // $ANTLR start "L"
    public final void mL() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:979:11: ( ( 'l' | 'L' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:979:13: ( 'l' | 'L' )
            {
            if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "L"

    // $ANTLR start "M"
    public final void mM() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:980:11: ( ( 'm' | 'M' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:980:13: ( 'm' | 'M' )
            {
            if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "M"

    // $ANTLR start "N"
    public final void mN() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:981:11: ( ( 'n' | 'N' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:981:13: ( 'n' | 'N' )
            {
            if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "N"

    // $ANTLR start "O"
    public final void mO() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:982:11: ( ( 'o' | 'O' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:982:13: ( 'o' | 'O' )
            {
            if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "O"

    // $ANTLR start "P"
    public final void mP() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:983:11: ( ( 'p' | 'P' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:983:13: ( 'p' | 'P' )
            {
            if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "P"

    // $ANTLR start "Q"
    public final void mQ() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:984:11: ( ( 'q' | 'Q' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:984:13: ( 'q' | 'Q' )
            {
            if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Q"

    // $ANTLR start "R"
    public final void mR() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:985:11: ( ( 'r' | 'R' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:985:13: ( 'r' | 'R' )
            {
            if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "R"

    // $ANTLR start "S"
    public final void mS() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:986:11: ( ( 's' | 'S' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:986:13: ( 's' | 'S' )
            {
            if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "S"

    // $ANTLR start "T"
    public final void mT() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:987:11: ( ( 't' | 'T' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:987:13: ( 't' | 'T' )
            {
            if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "T"

    // $ANTLR start "U"
    public final void mU() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:988:11: ( ( 'u' | 'U' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:988:13: ( 'u' | 'U' )
            {
            if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "U"

    // $ANTLR start "V"
    public final void mV() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:989:11: ( ( 'v' | 'V' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:989:13: ( 'v' | 'V' )
            {
            if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "V"

    // $ANTLR start "W"
    public final void mW() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:990:11: ( ( 'w' | 'W' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:990:13: ( 'w' | 'W' )
            {
            if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "W"

    // $ANTLR start "X"
    public final void mX() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:991:11: ( ( 'x' | 'X' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:991:13: ( 'x' | 'X' )
            {
            if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "X"

    // $ANTLR start "Y"
    public final void mY() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:992:11: ( ( 'y' | 'Y' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:992:13: ( 'y' | 'Y' )
            {
            if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Y"

    // $ANTLR start "Z"
    public final void mZ() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:993:11: ( ( 'z' | 'Z' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:993:13: ( 'z' | 'Z' )
            {
            if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Z"

    // $ANTLR start "STRING_LITERAL"
    public final void mSTRING_LITERAL() throws RecognitionException {
        try {
            int _type = STRING_LITERAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            int c;

             StringBuilder b = new StringBuilder(); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:998:5: ( '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\'' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:998:7: '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\''
            {
            match('\''); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:998:12: (c=~ ( '\\'' ) | '\\'' '\\'' )*
            loop3:
            do {
                int alt3=3;
                int LA3_0 = input.LA(1);

                if ( (LA3_0=='\'') ) {
                    int LA3_1 = input.LA(2);

                    if ( (LA3_1=='\'') ) {
                        alt3=2;
                    }


                }
                else if ( ((LA3_0>='\u0000' && LA3_0<='&')||(LA3_0>='(' && LA3_0<='\uFFFF')) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:998:13: c=~ ( '\\'' )
            	    {
            	    c= input.LA(1);
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}

            	     b.appendCodePoint(c);

            	    }
            	    break;
            	case 2 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:998:50: '\\'' '\\''
            	    {
            	    match('\''); 
            	    match('\''); 
            	     b.appendCodePoint('\''); 

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);

            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
             setText(b.toString());     }
        finally {
        }
    }
    // $ANTLR end "STRING_LITERAL"

    // $ANTLR start "QUOTED_NAME"
    public final void mQUOTED_NAME() throws RecognitionException {
        try {
            int _type = QUOTED_NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            int c;

             StringBuilder b = new StringBuilder(); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1004:5: ( '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )* '\\\"' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1004:7: '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )* '\\\"'
            {
            match('\"'); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1004:12: (c=~ ( '\\\"' ) | '\\\"' '\\\"' )*
            loop4:
            do {
                int alt4=3;
                int LA4_0 = input.LA(1);

                if ( (LA4_0=='\"') ) {
                    int LA4_1 = input.LA(2);

                    if ( (LA4_1=='\"') ) {
                        alt4=2;
                    }


                }
                else if ( ((LA4_0>='\u0000' && LA4_0<='!')||(LA4_0>='#' && LA4_0<='\uFFFF')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1004:13: c=~ ( '\\\"' )
            	    {
            	    c= input.LA(1);
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='!')||(input.LA(1)>='#' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}

            	     b.appendCodePoint(c); 

            	    }
            	    break;
            	case 2 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1004:51: '\\\"' '\\\"'
            	    {
            	    match('\"'); 
            	    match('\"'); 
            	     b.appendCodePoint('\"'); 

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);

            match('\"'); 

            }

            state.type = _type;
            state.channel = _channel;
             setText(b.toString());     }
        finally {
        }
    }
    // $ANTLR end "QUOTED_NAME"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1008:5: ( '0' .. '9' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1008:7: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    // $ANTLR start "LETTER"
    public final void mLETTER() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1012:5: ( ( 'A' .. 'Z' | 'a' .. 'z' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1012:7: ( 'A' .. 'Z' | 'a' .. 'z' )
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "LETTER"

    // $ANTLR start "HEX"
    public final void mHEX() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1016:5: ( ( 'A' .. 'F' | 'a' .. 'f' | '0' .. '9' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1016:7: ( 'A' .. 'F' | 'a' .. 'f' | '0' .. '9' )
            {
            if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='F')||(input.LA(1)>='a' && input.LA(1)<='f') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "HEX"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1020:5: ( E ( '+' | '-' )? ( DIGIT )+ )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1020:7: E ( '+' | '-' )? ( DIGIT )+
            {
            mE(); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1020:9: ( '+' | '-' )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0=='+'||LA5_0=='-') ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1020:22: ( DIGIT )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>='0' && LA6_0<='9')) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1020:22: DIGIT
            	    {
            	    mDIGIT(); 

            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "EXPONENT"

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:5: ( ( '-' )? ( DIGIT )+ )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:7: ( '-' )? ( DIGIT )+
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:7: ( '-' )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0=='-') ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:7: '-'
                    {
                    match('-'); 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:12: ( DIGIT )+
            int cnt8=0;
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( ((LA8_0>='0' && LA8_0<='9')) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1024:12: DIGIT
            	    {
            	    mDIGIT(); 

            	    }
            	    break;

            	default :
            	    if ( cnt8 >= 1 ) break loop8;
                        EarlyExitException eee =
                            new EarlyExitException(8, input);
                        throw eee;
                }
                cnt8++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INTEGER"

    // $ANTLR start "QMARK"
    public final void mQMARK() throws RecognitionException {
        try {
            int _type = QMARK;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1028:5: ( '?' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1028:7: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "QMARK"

    // $ANTLR start "FLOAT"
    public final void mFLOAT() throws RecognitionException {
        try {
            int _type = FLOAT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1036:5: ( INTEGER EXPONENT | INTEGER '.' ( DIGIT )* ( EXPONENT )? )
            int alt11=2;
            alt11 = dfa11.predict(input);
            switch (alt11) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1036:7: INTEGER EXPONENT
                    {
                    mINTEGER(); 
                    mEXPONENT(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1037:7: INTEGER '.' ( DIGIT )* ( EXPONENT )?
                    {
                    mINTEGER(); 
                    match('.'); 
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1037:19: ( DIGIT )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( ((LA9_0>='0' && LA9_0<='9')) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1037:19: DIGIT
                    	    {
                    	    mDIGIT(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);

                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1037:26: ( EXPONENT )?
                    int alt10=2;
                    int LA10_0 = input.LA(1);

                    if ( (LA10_0=='E'||LA10_0=='e') ) {
                        alt10=1;
                    }
                    switch (alt10) {
                        case 1 :
                            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1037:26: EXPONENT
                            {
                            mEXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FLOAT"

    // $ANTLR start "BOOLEAN"
    public final void mBOOLEAN() throws RecognitionException {
        try {
            int _type = BOOLEAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1044:5: ( T R U E | F A L S E )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0=='T'||LA12_0=='t') ) {
                alt12=1;
            }
            else if ( (LA12_0=='F'||LA12_0=='f') ) {
                alt12=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1044:7: T R U E
                    {
                    mT(); 
                    mR(); 
                    mU(); 
                    mE(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1044:17: F A L S E
                    {
                    mF(); 
                    mA(); 
                    mL(); 
                    mS(); 
                    mE(); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BOOLEAN"

    // $ANTLR start "IDENT"
    public final void mIDENT() throws RecognitionException {
        try {
            int _type = IDENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1048:5: ( LETTER ( LETTER | DIGIT | '_' )* )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1048:7: LETTER ( LETTER | DIGIT | '_' )*
            {
            mLETTER(); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1048:14: ( LETTER | DIGIT | '_' )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( ((LA13_0>='0' && LA13_0<='9')||(LA13_0>='A' && LA13_0<='Z')||LA13_0=='_'||(LA13_0>='a' && LA13_0<='z')) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
            	    {
            	    if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "IDENT"

    // $ANTLR start "HEXNUMBER"
    public final void mHEXNUMBER() throws RecognitionException {
        try {
            int _type = HEXNUMBER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1052:5: ( '0' X ( HEX )+ )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1052:7: '0' X ( HEX )+
            {
            match('0'); 
            mX(); 
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1052:13: ( HEX )+
            int cnt14=0;
            loop14:
            do {
                int alt14=2;
                int LA14_0 = input.LA(1);

                if ( ((LA14_0>='0' && LA14_0<='9')||(LA14_0>='A' && LA14_0<='F')||(LA14_0>='a' && LA14_0<='f')) ) {
                    alt14=1;
                }


                switch (alt14) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1052:13: HEX
            	    {
            	    mHEX(); 

            	    }
            	    break;

            	default :
            	    if ( cnt14 >= 1 ) break loop14;
                        EarlyExitException eee =
                            new EarlyExitException(14, input);
                        throw eee;
                }
                cnt14++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "HEXNUMBER"

    // $ANTLR start "UUID"
    public final void mUUID() throws RecognitionException {
        try {
            int _type = UUID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1056:5: ( HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1056:7: HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
            {
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            match('-'); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            match('-'); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            match('-'); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            match('-'); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 
            mHEX(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "UUID"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1064:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1064:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1064:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            int cnt15=0;
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( ((LA15_0>='\t' && LA15_0<='\n')||LA15_0=='\r'||LA15_0==' ') ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
            	    {
            	    if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


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

             _channel = HIDDEN; 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:5: ( ( '--' | '//' ) ( . )* ( '\\n' | '\\r' ) )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:7: ( '--' | '//' ) ( . )* ( '\\n' | '\\r' )
            {
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:7: ( '--' | '//' )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0=='-') ) {
                alt16=1;
            }
            else if ( (LA16_0=='/') ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:8: '--'
                    {
                    match("--"); 


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:15: '//'
                    {
                    match("//"); 


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:21: ( . )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0=='\n'||LA17_0=='\r') ) {
                    alt17=2;
                }
                else if ( ((LA17_0>='\u0000' && LA17_0<='\t')||(LA17_0>='\u000B' && LA17_0<='\f')||(LA17_0>='\u000E' && LA17_0<='\uFFFF')) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1068:21: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);

            if ( input.LA(1)=='\n'||input.LA(1)=='\r' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

             _channel = HIDDEN; 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "MULTILINE_COMMENT"
    public final void mMULTILINE_COMMENT() throws RecognitionException {
        try {
            int _type = MULTILINE_COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1072:5: ( '/*' ( . )* '*/' )
            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1072:7: '/*' ( . )* '*/'
            {
            match("/*"); 

            // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1072:12: ( . )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( (LA18_0=='*') ) {
                    int LA18_1 = input.LA(2);

                    if ( (LA18_1=='/') ) {
                        alt18=2;
                    }
                    else if ( ((LA18_1>='\u0000' && LA18_1<='.')||(LA18_1>='0' && LA18_1<='\uFFFF')) ) {
                        alt18=1;
                    }


                }
                else if ( ((LA18_0>='\u0000' && LA18_0<=')')||(LA18_0>='+' && LA18_0<='\uFFFF')) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1072:12: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);

            match("*/"); 

             _channel = HIDDEN; 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "MULTILINE_COMMENT"

    public void mTokens() throws RecognitionException {
        // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:8: ( T__125 | T__126 | T__127 | T__128 | T__129 | T__130 | T__131 | T__132 | T__133 | T__134 | T__135 | T__136 | T__137 | T__138 | T__139 | T__140 | T__141 | T__142 | K_SELECT | K_FROM | K_WHERE | K_AND | K_KEY | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_USE | K_COUNT | K_SET | K_BEGIN | K_UNLOGGED | K_BATCH | K_APPLY | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_KEYSPACES | K_COLUMNFAMILY | K_INDEX | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_RENAME | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_ALLOW | K_FILTERING | K_GRANT | K_ALL | K_PERMISSION | K_PERMISSIONS | K_OF | K_REVOKE | K_MODIFY | K_AUTHORIZE | K_NORECURSIVE | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | K_MAP | K_LIST | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | BOOLEAN | IDENT | HEXNUMBER | UUID | WS | COMMENT | MULTILINE_COMMENT )
        int alt19=109;
        alt19 = dfa19.predict(input);
        switch (alt19) {
            case 1 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:10: T__125
                {
                mT__125(); 

                }
                break;
            case 2 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:17: T__126
                {
                mT__126(); 

                }
                break;
            case 3 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:24: T__127
                {
                mT__127(); 

                }
                break;
            case 4 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:31: T__128
                {
                mT__128(); 

                }
                break;
            case 5 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:38: T__129
                {
                mT__129(); 

                }
                break;
            case 6 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:45: T__130
                {
                mT__130(); 

                }
                break;
            case 7 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:52: T__131
                {
                mT__131(); 

                }
                break;
            case 8 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:59: T__132
                {
                mT__132(); 

                }
                break;
            case 9 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:66: T__133
                {
                mT__133(); 

                }
                break;
            case 10 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:73: T__134
                {
                mT__134(); 

                }
                break;
            case 11 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:80: T__135
                {
                mT__135(); 

                }
                break;
            case 12 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:87: T__136
                {
                mT__136(); 

                }
                break;
            case 13 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:94: T__137
                {
                mT__137(); 

                }
                break;
            case 14 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:101: T__138
                {
                mT__138(); 

                }
                break;
            case 15 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:108: T__139
                {
                mT__139(); 

                }
                break;
            case 16 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:115: T__140
                {
                mT__140(); 

                }
                break;
            case 17 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:122: T__141
                {
                mT__141(); 

                }
                break;
            case 18 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:129: T__142
                {
                mT__142(); 

                }
                break;
            case 19 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:136: K_SELECT
                {
                mK_SELECT(); 

                }
                break;
            case 20 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:145: K_FROM
                {
                mK_FROM(); 

                }
                break;
            case 21 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:152: K_WHERE
                {
                mK_WHERE(); 

                }
                break;
            case 22 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:160: K_AND
                {
                mK_AND(); 

                }
                break;
            case 23 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:166: K_KEY
                {
                mK_KEY(); 

                }
                break;
            case 24 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:172: K_INSERT
                {
                mK_INSERT(); 

                }
                break;
            case 25 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:181: K_UPDATE
                {
                mK_UPDATE(); 

                }
                break;
            case 26 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:190: K_WITH
                {
                mK_WITH(); 

                }
                break;
            case 27 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:197: K_LIMIT
                {
                mK_LIMIT(); 

                }
                break;
            case 28 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:205: K_USING
                {
                mK_USING(); 

                }
                break;
            case 29 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:213: K_USE
                {
                mK_USE(); 

                }
                break;
            case 30 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:219: K_COUNT
                {
                mK_COUNT(); 

                }
                break;
            case 31 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:227: K_SET
                {
                mK_SET(); 

                }
                break;
            case 32 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:233: K_BEGIN
                {
                mK_BEGIN(); 

                }
                break;
            case 33 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:241: K_UNLOGGED
                {
                mK_UNLOGGED(); 

                }
                break;
            case 34 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:252: K_BATCH
                {
                mK_BATCH(); 

                }
                break;
            case 35 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:260: K_APPLY
                {
                mK_APPLY(); 

                }
                break;
            case 36 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:268: K_TRUNCATE
                {
                mK_TRUNCATE(); 

                }
                break;
            case 37 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:279: K_DELETE
                {
                mK_DELETE(); 

                }
                break;
            case 38 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:288: K_IN
                {
                mK_IN(); 

                }
                break;
            case 39 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:293: K_CREATE
                {
                mK_CREATE(); 

                }
                break;
            case 40 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:302: K_KEYSPACE
                {
                mK_KEYSPACE(); 

                }
                break;
            case 41 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:313: K_KEYSPACES
                {
                mK_KEYSPACES(); 

                }
                break;
            case 42 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:325: K_COLUMNFAMILY
                {
                mK_COLUMNFAMILY(); 

                }
                break;
            case 43 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:340: K_INDEX
                {
                mK_INDEX(); 

                }
                break;
            case 44 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:348: K_ON
                {
                mK_ON(); 

                }
                break;
            case 45 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:353: K_TO
                {
                mK_TO(); 

                }
                break;
            case 46 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:358: K_DROP
                {
                mK_DROP(); 

                }
                break;
            case 47 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:365: K_PRIMARY
                {
                mK_PRIMARY(); 

                }
                break;
            case 48 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:375: K_INTO
                {
                mK_INTO(); 

                }
                break;
            case 49 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:382: K_VALUES
                {
                mK_VALUES(); 

                }
                break;
            case 50 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:391: K_TIMESTAMP
                {
                mK_TIMESTAMP(); 

                }
                break;
            case 51 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:403: K_TTL
                {
                mK_TTL(); 

                }
                break;
            case 52 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:409: K_ALTER
                {
                mK_ALTER(); 

                }
                break;
            case 53 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:417: K_RENAME
                {
                mK_RENAME(); 

                }
                break;
            case 54 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:426: K_ADD
                {
                mK_ADD(); 

                }
                break;
            case 55 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:432: K_TYPE
                {
                mK_TYPE(); 

                }
                break;
            case 56 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:439: K_COMPACT
                {
                mK_COMPACT(); 

                }
                break;
            case 57 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:449: K_STORAGE
                {
                mK_STORAGE(); 

                }
                break;
            case 58 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:459: K_ORDER
                {
                mK_ORDER(); 

                }
                break;
            case 59 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:467: K_BY
                {
                mK_BY(); 

                }
                break;
            case 60 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:472: K_ASC
                {
                mK_ASC(); 

                }
                break;
            case 61 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:478: K_DESC
                {
                mK_DESC(); 

                }
                break;
            case 62 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:485: K_ALLOW
                {
                mK_ALLOW(); 

                }
                break;
            case 63 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:493: K_FILTERING
                {
                mK_FILTERING(); 

                }
                break;
            case 64 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:505: K_GRANT
                {
                mK_GRANT(); 

                }
                break;
            case 65 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:513: K_ALL
                {
                mK_ALL(); 

                }
                break;
            case 66 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:519: K_PERMISSION
                {
                mK_PERMISSION(); 

                }
                break;
            case 67 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:532: K_PERMISSIONS
                {
                mK_PERMISSIONS(); 

                }
                break;
            case 68 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:546: K_OF
                {
                mK_OF(); 

                }
                break;
            case 69 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:551: K_REVOKE
                {
                mK_REVOKE(); 

                }
                break;
            case 70 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:560: K_MODIFY
                {
                mK_MODIFY(); 

                }
                break;
            case 71 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:569: K_AUTHORIZE
                {
                mK_AUTHORIZE(); 

                }
                break;
            case 72 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:581: K_NORECURSIVE
                {
                mK_NORECURSIVE(); 

                }
                break;
            case 73 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:595: K_USER
                {
                mK_USER(); 

                }
                break;
            case 74 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:602: K_USERS
                {
                mK_USERS(); 

                }
                break;
            case 75 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:610: K_SUPERUSER
                {
                mK_SUPERUSER(); 

                }
                break;
            case 76 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:622: K_NOSUPERUSER
                {
                mK_NOSUPERUSER(); 

                }
                break;
            case 77 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:636: K_PASSWORD
                {
                mK_PASSWORD(); 

                }
                break;
            case 78 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:647: K_CLUSTERING
                {
                mK_CLUSTERING(); 

                }
                break;
            case 79 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:660: K_ASCII
                {
                mK_ASCII(); 

                }
                break;
            case 80 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:668: K_BIGINT
                {
                mK_BIGINT(); 

                }
                break;
            case 81 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:677: K_BLOB
                {
                mK_BLOB(); 

                }
                break;
            case 82 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:684: K_BOOLEAN
                {
                mK_BOOLEAN(); 

                }
                break;
            case 83 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:694: K_COUNTER
                {
                mK_COUNTER(); 

                }
                break;
            case 84 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:704: K_DECIMAL
                {
                mK_DECIMAL(); 

                }
                break;
            case 85 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:714: K_DOUBLE
                {
                mK_DOUBLE(); 

                }
                break;
            case 86 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:723: K_FLOAT
                {
                mK_FLOAT(); 

                }
                break;
            case 87 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:731: K_INET
                {
                mK_INET(); 

                }
                break;
            case 88 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:738: K_INT
                {
                mK_INT(); 

                }
                break;
            case 89 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:744: K_TEXT
                {
                mK_TEXT(); 

                }
                break;
            case 90 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:751: K_UUID
                {
                mK_UUID(); 

                }
                break;
            case 91 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:758: K_VARCHAR
                {
                mK_VARCHAR(); 

                }
                break;
            case 92 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:768: K_VARINT
                {
                mK_VARINT(); 

                }
                break;
            case 93 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:777: K_TIMEUUID
                {
                mK_TIMEUUID(); 

                }
                break;
            case 94 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:788: K_TOKEN
                {
                mK_TOKEN(); 

                }
                break;
            case 95 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:796: K_WRITETIME
                {
                mK_WRITETIME(); 

                }
                break;
            case 96 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:808: K_MAP
                {
                mK_MAP(); 

                }
                break;
            case 97 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:814: K_LIST
                {
                mK_LIST(); 

                }
                break;
            case 98 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:821: STRING_LITERAL
                {
                mSTRING_LITERAL(); 

                }
                break;
            case 99 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:836: QUOTED_NAME
                {
                mQUOTED_NAME(); 

                }
                break;
            case 100 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:848: INTEGER
                {
                mINTEGER(); 

                }
                break;
            case 101 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:856: QMARK
                {
                mQMARK(); 

                }
                break;
            case 102 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:862: FLOAT
                {
                mFLOAT(); 

                }
                break;
            case 103 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:868: BOOLEAN
                {
                mBOOLEAN(); 

                }
                break;
            case 104 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:876: IDENT
                {
                mIDENT(); 

                }
                break;
            case 105 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:882: HEXNUMBER
                {
                mHEXNUMBER(); 

                }
                break;
            case 106 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:892: UUID
                {
                mUUID(); 

                }
                break;
            case 107 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:897: WS
                {
                mWS(); 

                }
                break;
            case 108 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:900: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 109 :
                // /Users/WesleyChow/Documents/Berkeley/Research/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:908: MULTILINE_COMMENT
                {
                mMULTILINE_COMMENT(); 

                }
                break;

        }

    }


    protected DFA11 dfa11 = new DFA11(this);
    protected DFA19 dfa19 = new DFA19(this);
    static final String DFA11_eotS =
        "\5\uffff";
    static final String DFA11_eofS =
        "\5\uffff";
    static final String DFA11_minS =
        "\1\55\1\60\1\56\2\uffff";
    static final String DFA11_maxS =
        "\2\71\1\145\2\uffff";
    static final String DFA11_acceptS =
        "\3\uffff\1\1\1\2";
    static final String DFA11_specialS =
        "\5\uffff}>";
    static final String[] DFA11_transitionS = {
            "\1\1\2\uffff\12\2",
            "\12\2",
            "\1\4\1\uffff\12\2\13\uffff\1\3\37\uffff\1\3",
            "",
            ""
    };

    static final short[] DFA11_eot = DFA.unpackEncodedString(DFA11_eotS);
    static final short[] DFA11_eof = DFA.unpackEncodedString(DFA11_eofS);
    static final char[] DFA11_min = DFA.unpackEncodedStringToUnsignedChars(DFA11_minS);
    static final char[] DFA11_max = DFA.unpackEncodedStringToUnsignedChars(DFA11_maxS);
    static final short[] DFA11_accept = DFA.unpackEncodedString(DFA11_acceptS);
    static final short[] DFA11_special = DFA.unpackEncodedString(DFA11_specialS);
    static final short[][] DFA11_transition;

    static {
        int numStates = DFA11_transitionS.length;
        DFA11_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA11_transition[i] = DFA.unpackEncodedString(DFA11_transitionS[i]);
        }
    }

    class DFA11 extends DFA {

        public DFA11(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 11;
            this.eot = DFA11_eot;
            this.eof = DFA11_eof;
            this.min = DFA11_min;
            this.max = DFA11_max;
            this.accept = DFA11_accept;
            this.special = DFA11_special;
            this.transition = DFA11_transition;
        }
        public String getDescription() {
            return "1035:1: FLOAT : ( INTEGER EXPONENT | INTEGER '.' ( DIGIT )* ( EXPONENT )? );";
        }
    }
    static final String DFA19_eotS =
        "\16\uffff\1\57\1\61\1\63\23\52\2\uffff\1\154\1\uffff\1\52\1\154"+
        "\4\uffff\1\154\5\uffff\23\52\1\u0088\11\52\1\u009a\7\52\1\u00a2"+
        "\6\52\1\u00ac\1\52\1\u00ae\11\52\1\uffff\1\154\5\uffff\2\52\1\u00c0"+
        "\12\52\1\u00cb\1\52\1\u00ce\1\u00cf\1\52\1\u00d1\1\52\1\u00d4\1"+
        "\uffff\1\u00d6\4\52\1\u00dc\13\52\1\uffff\6\52\1\u00ef\1\uffff\11"+
        "\52\1\uffff\1\52\1\uffff\11\52\1\u0105\2\52\1\154\1\uffff\1\160"+
        "\2\52\1\uffff\5\52\1\u0112\3\52\1\u0116\1\uffff\2\52\2\uffff\1\52"+
        "\1\uffff\2\52\1\uffff\1\52\1\uffff\1\u011d\2\52\1\u0120\1\u0121"+
        "\1\uffff\1\u0122\3\52\1\u0127\12\52\1\u0132\1\52\1\u0135\1\uffff"+
        "\1\52\1\u0137\1\u0138\3\52\1\u013c\2\52\1\u013f\13\52\1\uffff\2"+
        "\52\1\154\1\uffff\1\160\4\52\1\u0138\1\52\1\u0155\1\uffff\1\52\1"+
        "\u0157\1\52\1\uffff\1\u0159\1\u015a\1\u015b\1\u015c\2\52\1\uffff"+
        "\1\52\1\u0160\3\uffff\1\u0161\1\u0162\2\52\1\uffff\1\u0165\2\52"+
        "\1\u0168\2\52\1\u016c\1\u016d\2\52\1\uffff\2\52\1\uffff\1\u0172"+
        "\2\uffff\1\52\1\u0174\1\52\1\uffff\2\52\1\uffff\1\u0178\10\52\1"+
        "\u0181\3\52\1\154\1\uffff\1\160\1\52\1\u0189\1\52\1\u018b\1\52\1"+
        "\uffff\1\52\1\uffff\1\52\4\uffff\2\52\1\u0191\3\uffff\1\u0192\1"+
        "\52\1\uffff\1\u0194\1\52\1\uffff\3\52\2\uffff\1\52\1\u019a\2\52"+
        "\1\uffff\1\52\1\uffff\1\u019e\1\52\1\u01a0\1\uffff\3\52\1\u01a4"+
        "\1\u01a5\1\52\1\u01a7\1\u01a8\1\uffff\1\u01a9\2\52\1\154\1\uffff"+
        "\1\160\1\52\1\uffff\1\u01b0\1\uffff\5\52\2\uffff\1\52\1\uffff\1"+
        "\52\1\u01b8\1\u01b9\1\52\1\u01bb\1\uffff\3\52\1\uffff\1\u01bf\1"+
        "\uffff\1\u01c0\2\52\2\uffff\1\u01c3\3\uffff\2\52\1\154\1\uffff\1"+
        "\160\1\52\1\uffff\4\52\1\u018b\1\u01ce\1\52\2\uffff\1\52\1\uffff"+
        "\1\u01d1\1\52\1\u01d3\2\uffff\1\52\1\u01d5\1\uffff\2\52\1\154\1"+
        "\uffff\1\160\1\u01d9\1\u01da\1\u01db\1\u01dc\1\u01dd\1\uffff\2\52"+
        "\1\uffff\1\u01e0\1\uffff\1\52\1\uffff\2\52\6\uffff\1\u01e5\1\52"+
        "\1\uffff\1\u01e7\2\52\1\160\1\uffff\1\52\1\uffff\1\u01ed\1\u01ee"+
        "\1\u01ef\1\160\1\u0174\3\uffff\2\160";
    static final String DFA19_eofS =
        "\u01f2\uffff";
    static final String DFA19_minS =
        "\1\11\15\uffff\1\55\2\75\1\103\1\60\1\110\1\60\1\105\2\116\1\111"+
        "\2\60\1\101\1\60\1\106\2\101\1\105\1\122\1\101\1\117\2\uffff\1\56"+
        "\1\uffff\1\60\1\56\2\uffff\1\52\1\uffff\1\56\5\uffff\1\120\1\114"+
        "\1\117\1\110\1\60\1\117\1\60\1\117\1\114\1\105\1\111\1\124\1\114"+
        "\1\104\1\60\1\120\1\103\1\124\1\131\1\60\1\111\1\105\1\104\1\114"+
        "\1\115\1\105\1\125\1\114\3\60\1\117\1\107\1\117\1\115\1\130\1\114"+
        "\1\60\1\120\1\125\1\102\1\60\1\125\1\117\1\60\1\104\1\60\1\111\1"+
        "\122\1\123\1\114\1\116\1\101\1\104\1\120\1\122\1\uffff\1\56\1\53"+
        "\4\uffff\2\105\1\60\1\122\1\105\1\123\1\60\1\101\1\115\1\124\1\122"+
        "\1\124\1\110\1\60\1\105\2\60\1\114\1\60\1\110\1\60\1\uffff\1\60"+
        "\2\105\1\124\1\104\1\60\1\116\1\101\1\117\1\124\1\111\1\101\1\123"+
        "\1\116\1\120\1\125\1\103\1\uffff\1\111\1\114\1\111\1\102\1\105\1"+
        "\124\1\60\1\uffff\3\105\1\114\1\105\1\103\1\60\1\102\1\120\1\uffff"+
        "\1\105\1\uffff\2\115\1\123\1\125\1\103\1\117\1\101\1\116\1\111\1"+
        "\60\1\105\1\125\1\56\1\53\1\60\1\122\1\103\1\uffff\1\101\1\115\1"+
        "\105\1\60\1\124\1\60\3\105\1\60\1\uffff\1\127\1\122\2\uffff\1\131"+
        "\1\uffff\1\111\1\117\1\uffff\1\120\1\uffff\1\60\1\122\1\130\2\60"+
        "\1\uffff\1\60\1\107\1\124\1\107\1\60\4\124\1\101\1\115\1\110\1\116"+
        "\1\105\1\116\1\60\1\123\1\60\1\uffff\1\116\2\60\1\103\1\105\1\124"+
        "\1\60\1\115\1\114\1\60\1\122\1\101\1\111\1\127\1\105\1\116\1\110"+
        "\1\113\1\115\1\124\1\106\1\uffff\1\103\1\120\1\56\1\53\1\60\1\125"+
        "\1\124\1\107\1\101\3\60\1\uffff\1\122\1\60\1\124\1\uffff\4\60\1"+
        "\122\1\101\1\uffff\1\124\1\60\3\uffff\2\60\1\105\1\107\1\uffff\1"+
        "\60\2\105\1\60\1\103\1\116\2\60\1\101\1\124\1\uffff\1\125\1\124"+
        "\1\uffff\1\60\2\uffff\1\101\1\60\1\105\1\uffff\1\101\1\105\1\uffff"+
        "\1\60\1\122\1\123\1\117\1\123\1\124\1\101\2\105\1\60\1\131\1\125"+
        "\1\105\1\56\1\53\1\60\1\123\1\60\1\105\2\60\1\uffff\1\111\1\uffff"+
        "\1\111\4\uffff\1\111\1\103\1\60\3\uffff\1\60\1\105\1\uffff\1\60"+
        "\1\122\1\uffff\1\122\1\124\1\106\2\uffff\1\116\1\60\1\111\1\101"+
        "\1\uffff\1\124\1\uffff\1\60\1\114\1\60\1\uffff\1\131\1\123\1\122"+
        "\2\60\1\122\2\60\1\uffff\1\60\2\122\1\56\1\53\1\60\1\105\1\uffff"+
        "\1\60\1\uffff\1\60\1\116\1\115\1\132\1\105\2\uffff\1\104\1\uffff"+
        "\1\111\2\60\1\101\1\60\1\uffff\1\104\1\115\1\105\1\uffff\1\60\1"+
        "\uffff\1\60\1\111\1\104\2\uffff\1\60\3\uffff\1\123\1\125\1\56\1"+
        "\53\1\60\1\122\1\uffff\1\55\1\107\2\105\2\60\1\116\2\uffff\1\115"+
        "\1\uffff\1\60\1\120\1\60\2\uffff\1\117\1\60\1\uffff\1\111\1\123"+
        "\1\55\1\53\1\55\5\60\1\uffff\1\107\1\111\1\uffff\1\60\1\uffff\1"+
        "\116\1\uffff\1\126\1\105\1\60\5\uffff\1\60\1\114\1\uffff\1\60\1"+
        "\105\1\122\1\60\1\uffff\1\131\1\uffff\5\60\3\uffff\1\60\1\55";
    static final String DFA19_maxS =
        "\1\175\15\uffff\1\71\2\75\1\165\2\162\1\165\1\145\1\156\1\165\1"+
        "\151\1\162\2\171\3\162\1\141\1\145\1\162\2\157\2\uffff\1\170\1\uffff"+
        "\2\146\2\uffff\1\57\1\uffff\1\145\5\uffff\1\160\1\164\1\157\1\150"+
        "\1\154\1\157\1\146\1\157\1\154\1\145\1\151\2\164\1\144\1\146\1\160"+
        "\1\143\1\164\1\171\1\172\2\151\1\144\1\154\1\163\1\145\2\165\1\164"+
        "\1\172\1\147\1\157\1\147\1\157\1\155\1\170\1\154\1\172\1\160\1\165"+
        "\1\142\1\163\1\165\1\157\1\172\1\144\1\172\1\151\1\162\1\163\1\162"+
        "\1\166\1\141\1\144\1\160\1\163\1\uffff\2\146\4\uffff\2\145\1\172"+
        "\1\162\1\145\1\163\1\146\1\141\1\155\1\164\1\162\1\164\1\150\1\172"+
        "\1\145\2\172\1\154\1\172\1\150\1\172\1\uffff\1\172\2\145\1\164\1"+
        "\144\1\172\1\156\1\141\1\157\1\164\1\151\1\141\1\163\1\156\1\160"+
        "\1\165\1\143\1\uffff\1\151\1\154\1\151\1\142\1\145\1\164\1\172\1"+
        "\uffff\2\145\1\156\1\154\1\145\1\143\1\151\1\142\1\160\1\uffff\1"+
        "\145\1\uffff\2\155\1\163\1\165\1\151\1\157\1\141\1\156\1\151\1\172"+
        "\1\145\1\165\3\146\1\162\1\143\1\uffff\1\141\1\155\1\145\1\146\1"+
        "\164\1\172\3\145\1\172\1\uffff\1\167\1\162\2\uffff\1\171\1\uffff"+
        "\1\151\1\157\1\uffff\1\160\1\uffff\1\172\1\162\1\170\2\172\1\uffff"+
        "\1\172\1\147\1\164\1\147\1\172\4\164\1\141\1\155\1\150\1\156\1\145"+
        "\1\156\1\172\1\165\1\172\1\uffff\1\156\2\172\1\143\1\145\1\164\1"+
        "\172\1\155\1\154\1\172\1\162\1\141\1\151\1\167\1\145\1\156\1\150"+
        "\1\153\1\155\1\164\1\146\1\uffff\1\143\1\160\3\146\1\165\1\164\1"+
        "\147\1\141\1\172\1\146\1\172\1\uffff\1\162\1\172\1\164\1\uffff\4"+
        "\172\1\162\1\141\1\uffff\1\164\1\172\3\uffff\2\172\1\145\1\147\1"+
        "\uffff\1\172\2\145\1\172\1\143\1\156\2\172\1\141\1\164\1\uffff\1"+
        "\165\1\164\1\uffff\1\172\2\uffff\1\141\1\172\1\145\1\uffff\1\141"+
        "\1\145\1\uffff\1\172\1\162\1\163\1\157\1\163\1\164\1\141\2\145\1"+
        "\172\1\171\1\165\1\145\3\146\1\163\1\172\1\145\1\172\1\146\1\uffff"+
        "\1\151\1\uffff\1\151\4\uffff\1\151\1\143\1\172\3\uffff\1\172\1\145"+
        "\1\uffff\1\172\1\162\1\uffff\1\162\1\164\1\146\2\uffff\1\156\1\172"+
        "\1\151\1\141\1\uffff\1\164\1\uffff\1\172\1\154\1\172\1\uffff\1\171"+
        "\1\163\1\162\2\172\1\162\2\172\1\uffff\1\172\2\162\3\146\1\145\1"+
        "\uffff\1\172\1\uffff\1\146\1\156\1\155\1\172\1\145\2\uffff\1\144"+
        "\1\uffff\1\151\2\172\1\141\1\172\1\uffff\1\144\1\155\1\145\1\uffff"+
        "\1\172\1\uffff\1\172\1\151\1\144\2\uffff\1\172\3\uffff\1\163\1\165"+
        "\3\146\1\162\1\uffff\1\55\1\147\2\145\2\172\1\156\2\uffff\1\155"+
        "\1\uffff\1\172\1\160\1\172\2\uffff\1\157\1\172\1\uffff\1\151\1\163"+
        "\1\145\1\71\1\55\5\172\1\uffff\1\147\1\151\1\uffff\1\172\1\uffff"+
        "\1\156\1\uffff\1\166\1\145\1\146\5\uffff\1\172\1\154\1\uffff\1\172"+
        "\1\145\1\162\1\146\1\uffff\1\171\1\uffff\3\172\1\146\1\172\3\uffff"+
        "\1\146\1\55";
    static final String DFA19_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14\1"+
        "\15\26\uffff\1\142\1\143\1\uffff\1\145\2\uffff\1\150\1\153\1\uffff"+
        "\1\154\1\uffff\1\16\1\20\1\17\1\22\1\21\70\uffff\1\144\2\uffff\1"+
        "\152\1\146\1\151\1\155\25\uffff\1\46\21\uffff\1\73\7\uffff\1\55"+
        "\11\uffff\1\54\1\uffff\1\104\21\uffff\1\37\12\uffff\1\101\2\uffff"+
        "\1\26\1\66\1\uffff\1\74\2\uffff\1\27\1\uffff\1\130\5\uffff\1\35"+
        "\22\uffff\1\63\25\uffff\1\140\14\uffff\1\24\3\uffff\1\32\6\uffff"+
        "\1\60\2\uffff\1\127\1\132\1\111\4\uffff\1\141\12\uffff\1\121\2\uffff"+
        "\1\131\1\uffff\1\67\1\147\3\uffff\1\75\2\uffff\1\56\25\uffff\1\126"+
        "\1\uffff\1\25\1\uffff\1\76\1\64\1\43\1\117\3\uffff\1\53\1\112\1"+
        "\34\2\uffff\1\33\2\uffff\1\36\3\uffff\1\42\1\40\4\uffff\1\136\1"+
        "\uffff\1\52\3\uffff\1\72\10\uffff\1\100\7\uffff\1\23\1\uffff\1\50"+
        "\5\uffff\1\30\1\31\1\uffff\1\47\5\uffff\1\120\3\uffff\1\45\1\uffff"+
        "\1\125\3\uffff\1\61\1\134\1\uffff\1\105\1\65\1\106\6\uffff\1\71"+
        "\7\uffff\1\123\1\70\1\uffff\1\122\3\uffff\1\124\1\57\2\uffff\1\133"+
        "\12\uffff\1\41\2\uffff\1\135\1\uffff\1\44\1\uffff\1\115\3\uffff"+
        "\1\113\1\77\1\137\1\107\1\51\2\uffff\1\62\4\uffff\1\116\1\uffff"+
        "\1\102\5\uffff\1\103\1\110\1\114\2\uffff";
    static final String DFA19_specialS =
        "\u01f2\uffff}>";
    static final String[] DFA19_transitionS = {
            "\2\53\2\uffff\1\53\22\uffff\1\53\1\uffff\1\45\4\uffff\1\44\1"+
            "\2\1\3\1\5\1\15\1\4\1\16\1\10\1\54\1\46\11\51\1\12\1\1\1\17"+
            "\1\14\1\20\1\47\1\uffff\1\24\1\32\1\31\1\34\1\50\1\22\1\41\1"+
            "\52\1\26\1\52\1\25\1\30\1\42\1\43\1\35\1\36\1\52\1\40\1\21\1"+
            "\33\1\27\1\37\1\23\3\52\1\6\1\uffff\1\7\3\uffff\1\24\1\32\1"+
            "\31\1\34\1\50\1\22\1\41\1\52\1\26\1\52\1\25\1\30\1\42\1\43\1"+
            "\35\1\36\1\52\1\40\1\21\1\33\1\27\1\37\1\23\3\52\1\13\1\uffff"+
            "\1\11",
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
            "",
            "\1\55\2\uffff\12\56",
            "\1\60",
            "\1\62",
            "\1\67\1\uffff\1\65\16\uffff\1\66\1\64\15\uffff\1\67\1\uffff"+
            "\1\65\16\uffff\1\66\1\64",
            "\12\72\7\uffff\1\70\5\72\2\uffff\1\74\2\uffff\1\71\5\uffff"+
            "\1\73\16\uffff\1\70\5\72\2\uffff\1\74\2\uffff\1\71\5\uffff\1"+
            "\73",
            "\1\75\1\77\10\uffff\1\76\25\uffff\1\75\1\77\10\uffff\1\76",
            "\12\72\7\uffff\3\72\1\102\2\72\5\uffff\1\100\1\uffff\1\101"+
            "\1\uffff\1\103\2\uffff\1\104\1\uffff\1\105\13\uffff\3\72\1\102"+
            "\2\72\5\uffff\1\100\1\uffff\1\101\1\uffff\1\103\2\uffff\1\104"+
            "\1\uffff\1\105",
            "\1\106\37\uffff\1\106",
            "\1\107\37\uffff\1\107",
            "\1\113\1\uffff\1\112\2\uffff\1\111\1\uffff\1\110\30\uffff\1"+
            "\113\1\uffff\1\112\2\uffff\1\111\1\uffff\1\110",
            "\1\114\37\uffff\1\114",
            "\12\72\7\uffff\6\72\5\uffff\1\116\2\uffff\1\117\2\uffff\1\115"+
            "\16\uffff\6\72\5\uffff\1\116\2\uffff\1\117\2\uffff\1\115",
            "\12\72\7\uffff\1\120\3\72\1\122\1\72\2\uffff\1\124\2\uffff"+
            "\1\125\2\uffff\1\123\11\uffff\1\121\7\uffff\1\120\3\72\1\122"+
            "\1\72\2\uffff\1\124\2\uffff\1\125\2\uffff\1\123\11\uffff\1\121",
            "\1\134\3\uffff\1\127\3\uffff\1\126\5\uffff\1\131\2\uffff\1"+
            "\133\1\uffff\1\130\4\uffff\1\132\7\uffff\1\134\3\uffff\1\127"+
            "\3\uffff\1\126\5\uffff\1\131\2\uffff\1\133\1\uffff\1\130\4\uffff"+
            "\1\132",
            "\12\72\7\uffff\4\72\1\135\1\72\10\uffff\1\136\2\uffff\1\137"+
            "\16\uffff\4\72\1\135\1\72\10\uffff\1\136\2\uffff\1\137",
            "\1\142\7\uffff\1\140\3\uffff\1\141\23\uffff\1\142\7\uffff\1"+
            "\140\3\uffff\1\141",
            "\1\145\3\uffff\1\144\14\uffff\1\143\16\uffff\1\145\3\uffff"+
            "\1\144\14\uffff\1\143",
            "\1\146\37\uffff\1\146",
            "\1\147\37\uffff\1\147",
            "\1\150\37\uffff\1\150",
            "\1\152\15\uffff\1\151\21\uffff\1\152\15\uffff\1\151",
            "\1\153\37\uffff\1\153",
            "",
            "",
            "\1\160\1\uffff\12\155\7\uffff\4\157\1\156\1\157\21\uffff\1"+
            "\161\10\uffff\4\157\1\156\1\157\21\uffff\1\161",
            "",
            "\12\72\7\uffff\6\72\32\uffff\6\72",
            "\1\160\1\uffff\12\155\7\uffff\4\157\1\156\1\157\32\uffff\4"+
            "\157\1\156\1\157",
            "",
            "",
            "\1\162\4\uffff\1\55",
            "",
            "\1\160\1\uffff\12\56\13\uffff\1\160\37\uffff\1\160",
            "",
            "",
            "",
            "",
            "",
            "\1\163\37\uffff\1\163",
            "\1\164\7\uffff\1\165\27\uffff\1\164\7\uffff\1\165",
            "\1\166\37\uffff\1\166",
            "\1\167\37\uffff\1\167",
            "\12\171\7\uffff\6\171\5\uffff\1\170\24\uffff\6\171\5\uffff"+
            "\1\170",
            "\1\172\37\uffff\1\172",
            "\12\171\7\uffff\6\171\32\uffff\6\171",
            "\1\173\37\uffff\1\173",
            "\1\174\37\uffff\1\174",
            "\1\175\37\uffff\1\175",
            "\1\176\37\uffff\1\176",
            "\1\177\37\uffff\1\177",
            "\1\u0080\7\uffff\1\u0081\27\uffff\1\u0080\7\uffff\1\u0081",
            "\1\u0082\37\uffff\1\u0082",
            "\12\171\7\uffff\3\171\1\u0083\2\171\32\uffff\3\171\1\u0083"+
            "\2\171",
            "\1\u0084\37\uffff\1\u0084",
            "\1\u0085\37\uffff\1\u0085",
            "\1\u0086\37\uffff\1\u0086",
            "\1\u0087\37\uffff\1\u0087",
            "\12\52\7\uffff\3\52\1\u008b\1\u008c\15\52\1\u008a\1\u0089\6"+
            "\52\4\uffff\1\52\1\uffff\3\52\1\u008b\1\u008c\15\52\1\u008a"+
            "\1\u0089\6\52",
            "\1\u008d\37\uffff\1\u008d",
            "\1\u008e\3\uffff\1\u008f\33\uffff\1\u008e\3\uffff\1\u008f",
            "\1\u0090\37\uffff\1\u0090",
            "\1\u0091\37\uffff\1\u0091",
            "\1\u0093\5\uffff\1\u0092\31\uffff\1\u0093\5\uffff\1\u0092",
            "\1\u0094\37\uffff\1\u0094",
            "\1\u0095\37\uffff\1\u0095",
            "\1\u0098\1\u0097\7\uffff\1\u0096\26\uffff\1\u0098\1\u0097\7"+
            "\uffff\1\u0096",
            "\12\171\7\uffff\6\171\15\uffff\1\u0099\14\uffff\6\171\15\uffff"+
            "\1\u0099",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\171\7\uffff\6\171\1\u009b\31\uffff\6\171\1\u009b",
            "\1\u009c\37\uffff\1\u009c",
            "\1\u009d\37\uffff\1\u009d",
            "\1\u009e\37\uffff\1\u009e",
            "\1\u009f\37\uffff\1\u009f",
            "\1\u00a0\37\uffff\1\u00a0",
            "\1\u00a1\37\uffff\1\u00a1",
            "\12\52\7\uffff\12\52\1\u00a3\17\52\4\uffff\1\52\1\uffff\12"+
            "\52\1\u00a3\17\52",
            "\1\u00a4\37\uffff\1\u00a4",
            "\1\u00a5\37\uffff\1\u00a5",
            "\1\u00a6\37\uffff\1\u00a6",
            "\12\171\7\uffff\2\171\1\u00a9\3\171\5\uffff\1\u00a7\6\uffff"+
            "\1\u00a8\15\uffff\2\171\1\u00a9\3\171\5\uffff\1\u00a7\6\uffff"+
            "\1\u00a8",
            "\1\u00aa\37\uffff\1\u00aa",
            "\1\u00ab\37\uffff\1\u00ab",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u00ad\37\uffff\1\u00ad",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u00af\37\uffff\1\u00af",
            "\1\u00b0\37\uffff\1\u00b0",
            "\1\u00b1\37\uffff\1\u00b1",
            "\1\u00b2\5\uffff\1\u00b3\31\uffff\1\u00b2\5\uffff\1\u00b3",
            "\1\u00b5\7\uffff\1\u00b4\27\uffff\1\u00b5\7\uffff\1\u00b4",
            "\1\u00b6\37\uffff\1\u00b6",
            "\1\u00b7\37\uffff\1\u00b7",
            "\1\u00b8\37\uffff\1\u00b8",
            "\1\u00b9\1\u00ba\36\uffff\1\u00b9\1\u00ba",
            "",
            "\1\160\1\uffff\12\u00bb\7\uffff\4\157\1\u00bc\1\157\32\uffff"+
            "\4\157\1\u00bc\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u00bd\7\uffff\6\157\32\uffff"+
            "\6\157",
            "",
            "",
            "",
            "",
            "\1\u00be\37\uffff\1\u00be",
            "\1\u00bf\37\uffff\1\u00bf",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u00c1\37\uffff\1\u00c1",
            "\1\u00c2\37\uffff\1\u00c2",
            "\1\u00c3\37\uffff\1\u00c3",
            "\12\u00c4\7\uffff\6\u00c4\32\uffff\6\u00c4",
            "\1\u00c5\37\uffff\1\u00c5",
            "\1\u00c6\37\uffff\1\u00c6",
            "\1\u00c7\37\uffff\1\u00c7",
            "\1\u00c8\37\uffff\1\u00c8",
            "\1\u00c9\37\uffff\1\u00c9",
            "\1\u00ca\37\uffff\1\u00ca",
            "\12\52\7\uffff\16\52\1\u00cc\13\52\4\uffff\1\52\1\uffff\16"+
            "\52\1\u00cc\13\52",
            "\1\u00cd\37\uffff\1\u00cd",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\u00c4\7\uffff\6\u00c4\24\52\4\uffff\1\52\1\uffff\6\u00c4"+
            "\24\52",
            "\1\u00d0\37\uffff\1\u00d0",
            "\12\52\7\uffff\10\52\1\u00d2\21\52\4\uffff\1\52\1\uffff\10"+
            "\52\1\u00d2\21\52",
            "\1\u00d3\37\uffff\1\u00d3",
            "\12\52\7\uffff\22\52\1\u00d5\7\52\4\uffff\1\52\1\uffff\22\52"+
            "\1\u00d5\7\52",
            "",
            "\12\52\7\uffff\16\52\1\u00d7\13\52\4\uffff\1\52\1\uffff\16"+
            "\52\1\u00d7\13\52",
            "\1\u00d8\37\uffff\1\u00d8",
            "\1\u00d9\37\uffff\1\u00d9",
            "\1\u00da\37\uffff\1\u00da",
            "\1\u00db\37\uffff\1\u00db",
            "\12\52\7\uffff\21\52\1\u00dd\10\52\4\uffff\1\52\1\uffff\21"+
            "\52\1\u00dd\10\52",
            "\1\u00de\37\uffff\1\u00de",
            "\1\u00df\37\uffff\1\u00df",
            "\1\u00e0\37\uffff\1\u00e0",
            "\1\u00e1\37\uffff\1\u00e1",
            "\1\u00e2\37\uffff\1\u00e2",
            "\1\u00e3\37\uffff\1\u00e3",
            "\1\u00e4\37\uffff\1\u00e4",
            "\1\u00e5\37\uffff\1\u00e5",
            "\1\u00e6\37\uffff\1\u00e6",
            "\1\u00e7\37\uffff\1\u00e7",
            "\1\u00e8\37\uffff\1\u00e8",
            "",
            "\1\u00e9\37\uffff\1\u00e9",
            "\1\u00ea\37\uffff\1\u00ea",
            "\1\u00eb\37\uffff\1\u00eb",
            "\1\u00ec\37\uffff\1\u00ec",
            "\1\u00ed\37\uffff\1\u00ed",
            "\1\u00ee\37\uffff\1\u00ee",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u00f0\37\uffff\1\u00f0",
            "\1\u00f1\37\uffff\1\u00f1",
            "\1\u00f2\10\uffff\1\u00f3\26\uffff\1\u00f2\10\uffff\1\u00f3",
            "\1\u00f4\37\uffff\1\u00f4",
            "\1\u00f5\37\uffff\1\u00f5",
            "\1\u00f6\37\uffff\1\u00f6",
            "\12\u00c4\7\uffff\6\u00c4\2\uffff\1\u00f7\27\uffff\6\u00c4"+
            "\2\uffff\1\u00f7",
            "\1\u00f8\37\uffff\1\u00f8",
            "\1\u00f9\37\uffff\1\u00f9",
            "",
            "\1\u00fa\37\uffff\1\u00fa",
            "",
            "\1\u00fb\37\uffff\1\u00fb",
            "\1\u00fc\37\uffff\1\u00fc",
            "\1\u00fd\37\uffff\1\u00fd",
            "\1\u00fe\37\uffff\1\u00fe",
            "\1\u0100\5\uffff\1\u00ff\31\uffff\1\u0100\5\uffff\1\u00ff",
            "\1\u0101\37\uffff\1\u0101",
            "\1\u0102\37\uffff\1\u0102",
            "\1\u0103\37\uffff\1\u0103",
            "\1\u0104\37\uffff\1\u0104",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0106\37\uffff\1\u0106",
            "\1\u0107\37\uffff\1\u0107",
            "\1\160\1\uffff\12\u0108\7\uffff\4\157\1\u0109\1\157\32\uffff"+
            "\4\157\1\u0109\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u010a\7\uffff\6\157\32\uffff"+
            "\6\157",
            "\12\u010a\7\uffff\6\157\32\uffff\6\157",
            "\1\u010b\37\uffff\1\u010b",
            "\1\u010c\37\uffff\1\u010c",
            "",
            "\1\u010d\37\uffff\1\u010d",
            "\1\u010e\37\uffff\1\u010e",
            "\1\u010f\37\uffff\1\u010f",
            "\12\u0110\7\uffff\6\u0110\32\uffff\6\u0110",
            "\1\u0111\37\uffff\1\u0111",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0113\37\uffff\1\u0113",
            "\1\u0114\37\uffff\1\u0114",
            "\1\u0115\37\uffff\1\u0115",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u0117\37\uffff\1\u0117",
            "\1\u0118\37\uffff\1\u0118",
            "",
            "",
            "\1\u0119\37\uffff\1\u0119",
            "",
            "\1\u011a\37\uffff\1\u011a",
            "\1\u011b\37\uffff\1\u011b",
            "",
            "\1\u011c\37\uffff\1\u011c",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u011e\37\uffff\1\u011e",
            "\1\u011f\37\uffff\1\u011f",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\52\7\uffff\22\52\1\u0123\7\52\4\uffff\1\52\1\uffff\22\52"+
            "\1\u0123\7\52",
            "\1\u0124\37\uffff\1\u0124",
            "\1\u0125\37\uffff\1\u0125",
            "\1\u0126\37\uffff\1\u0126",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0128\37\uffff\1\u0128",
            "\1\u0129\37\uffff\1\u0129",
            "\1\u012a\37\uffff\1\u012a",
            "\1\u012b\37\uffff\1\u012b",
            "\1\u012c\37\uffff\1\u012c",
            "\1\u012d\37\uffff\1\u012d",
            "\1\u012e\37\uffff\1\u012e",
            "\1\u012f\37\uffff\1\u012f",
            "\1\u0130\37\uffff\1\u0130",
            "\1\u0131\37\uffff\1\u0131",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0134\1\uffff\1\u0133\35\uffff\1\u0134\1\uffff\1\u0133",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u0136\37\uffff\1\u0136",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0139\37\uffff\1\u0139",
            "\1\u013a\37\uffff\1\u013a",
            "\1\u013b\37\uffff\1\u013b",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u013d\37\uffff\1\u013d",
            "\1\u013e\37\uffff\1\u013e",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0140\37\uffff\1\u0140",
            "\1\u0141\37\uffff\1\u0141",
            "\1\u0142\37\uffff\1\u0142",
            "\1\u0143\37\uffff\1\u0143",
            "\1\u0144\37\uffff\1\u0144",
            "\1\u0145\37\uffff\1\u0145",
            "\1\u0146\37\uffff\1\u0146",
            "\1\u0147\37\uffff\1\u0147",
            "\1\u0148\37\uffff\1\u0148",
            "\1\u0149\37\uffff\1\u0149",
            "\1\u014a\37\uffff\1\u014a",
            "",
            "\1\u014b\37\uffff\1\u014b",
            "\1\u014c\37\uffff\1\u014c",
            "\1\160\1\uffff\12\u014d\7\uffff\4\157\1\u014e\1\157\32\uffff"+
            "\4\157\1\u014e\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u014f\7\uffff\6\157\32\uffff"+
            "\6\157",
            "\12\u014f\7\uffff\6\157\32\uffff\6\157",
            "\1\u0150\37\uffff\1\u0150",
            "\1\u0151\37\uffff\1\u0151",
            "\1\u0152\37\uffff\1\u0152",
            "\1\u0153\37\uffff\1\u0153",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\u0154\7\uffff\6\u0154\32\uffff\6\u0154",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u0156\37\uffff\1\u0156",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0158\37\uffff\1\u0158",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u015d\37\uffff\1\u015d",
            "\1\u015e\37\uffff\1\u015e",
            "",
            "\1\u015f\37\uffff\1\u015f",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0163\37\uffff\1\u0163",
            "\1\u0164\37\uffff\1\u0164",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0166\37\uffff\1\u0166",
            "\1\u0167\37\uffff\1\u0167",
            "\12\52\7\uffff\4\52\1\u0169\25\52\4\uffff\1\52\1\uffff\4\52"+
            "\1\u0169\25\52",
            "\1\u016a\37\uffff\1\u016a",
            "\1\u016b\37\uffff\1\u016b",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u016e\37\uffff\1\u016e",
            "\1\u016f\37\uffff\1\u016f",
            "",
            "\1\u0170\37\uffff\1\u0170",
            "\1\u0171\37\uffff\1\u0171",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "\1\u0173\37\uffff\1\u0173",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0175\37\uffff\1\u0175",
            "",
            "\1\u0176\37\uffff\1\u0176",
            "\1\u0177\37\uffff\1\u0177",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0179\37\uffff\1\u0179",
            "\1\u017a\37\uffff\1\u017a",
            "\1\u017b\37\uffff\1\u017b",
            "\1\u017c\37\uffff\1\u017c",
            "\1\u017d\37\uffff\1\u017d",
            "\1\u017e\37\uffff\1\u017e",
            "\1\u017f\37\uffff\1\u017f",
            "\1\u0180\37\uffff\1\u0180",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0182\37\uffff\1\u0182",
            "\1\u0183\37\uffff\1\u0183",
            "\1\u0184\37\uffff\1\u0184",
            "\1\160\1\uffff\12\u0185\7\uffff\4\157\1\u0186\1\157\32\uffff"+
            "\4\157\1\u0186\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u0187\7\uffff\6\157\32\uffff"+
            "\6\157",
            "\12\u0187\7\uffff\6\157\32\uffff\6\157",
            "\1\u0188\37\uffff\1\u0188",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u018a\37\uffff\1\u018a",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\u018c\7\uffff\6\u018c\32\uffff\6\u018c",
            "",
            "\1\u018d\37\uffff\1\u018d",
            "",
            "\1\u018e\37\uffff\1\u018e",
            "",
            "",
            "",
            "",
            "\1\u018f\37\uffff\1\u018f",
            "\1\u0190\37\uffff\1\u0190",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0193\37\uffff\1\u0193",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0195\37\uffff\1\u0195",
            "",
            "\1\u0196\37\uffff\1\u0196",
            "\1\u0197\37\uffff\1\u0197",
            "\1\u0198\37\uffff\1\u0198",
            "",
            "",
            "\1\u0199\37\uffff\1\u0199",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u019b\37\uffff\1\u019b",
            "\1\u019c\37\uffff\1\u019c",
            "",
            "\1\u019d\37\uffff\1\u019d",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u019f\37\uffff\1\u019f",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u01a1\37\uffff\1\u01a1",
            "\1\u01a2\37\uffff\1\u01a2",
            "\1\u01a3\37\uffff\1\u01a3",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01a6\37\uffff\1\u01a6",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01aa\37\uffff\1\u01aa",
            "\1\u01ab\37\uffff\1\u01ab",
            "\1\160\1\uffff\12\u01ac\7\uffff\4\157\1\u01ad\1\157\32\uffff"+
            "\4\157\1\u01ad\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u01ae\7\uffff\6\157\32\uffff"+
            "\6\157",
            "\12\u01ae\7\uffff\6\157\32\uffff\6\157",
            "\1\u01af\37\uffff\1\u01af",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\u01b1\7\uffff\6\u01b1\32\uffff\6\u01b1",
            "\1\u01b2\37\uffff\1\u01b2",
            "\1\u01b3\37\uffff\1\u01b3",
            "\1\u01b4\37\uffff\1\u01b4",
            "\1\u01b5\37\uffff\1\u01b5",
            "",
            "",
            "\1\u01b6\37\uffff\1\u01b6",
            "",
            "\1\u01b7\37\uffff\1\u01b7",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01ba\37\uffff\1\u01ba",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u01bc\37\uffff\1\u01bc",
            "\1\u01bd\37\uffff\1\u01bd",
            "\1\u01be\37\uffff\1\u01be",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01c1\37\uffff\1\u01c1",
            "\1\u01c2\37\uffff\1\u01c2",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "",
            "\1\u01c4\37\uffff\1\u01c4",
            "\1\u01c5\37\uffff\1\u01c5",
            "\1\160\1\uffff\12\u01c6\7\uffff\4\157\1\u01c7\1\157\32\uffff"+
            "\4\157\1\u01c7\1\157",
            "\1\160\1\uffff\1\160\2\uffff\12\u01c8\7\uffff\6\157\32\uffff"+
            "\6\157",
            "\12\u01c8\7\uffff\6\157\32\uffff\6\157",
            "\1\u01c9\37\uffff\1\u01c9",
            "",
            "\1\157",
            "\1\u01ca\37\uffff\1\u01ca",
            "\1\u01cb\37\uffff\1\u01cb",
            "\1\u01cc\37\uffff\1\u01cc",
            "\12\52\7\uffff\22\52\1\u01cd\7\52\4\uffff\1\52\1\uffff\22\52"+
            "\1\u01cd\7\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01cf\37\uffff\1\u01cf",
            "",
            "",
            "\1\u01d0\37\uffff\1\u01d0",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01d2\37\uffff\1\u01d2",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "\1\u01d4\37\uffff\1\u01d4",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u01d6\37\uffff\1\u01d6",
            "\1\u01d7\37\uffff\1\u01d7",
            "\1\157\1\160\1\uffff\12\56\13\uffff\1\160\37\uffff\1\160",
            "\1\160\1\uffff\1\u01d8\2\uffff\12\160",
            "\1\157",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u01de\37\uffff\1\u01de",
            "\1\u01df\37\uffff\1\u01df",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u01e1\37\uffff\1\u01e1",
            "",
            "\1\u01e2\37\uffff\1\u01e2",
            "\1\u01e3\37\uffff\1\u01e3",
            "\12\u01e4\7\uffff\6\157\32\uffff\6\157",
            "",
            "",
            "",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u01e6\37\uffff\1\u01e6",
            "",
            "\12\52\7\uffff\22\52\1\u01e8\7\52\4\uffff\1\52\1\uffff\22\52"+
            "\1\u01e8\7\52",
            "\1\u01e9\37\uffff\1\u01e9",
            "\1\u01ea\37\uffff\1\u01ea",
            "\12\u01eb\7\uffff\6\157\32\uffff\6\157",
            "",
            "\1\u01ec\37\uffff\1\u01ec",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\u01f0\7\uffff\6\157\32\uffff\6\157",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "",
            "\12\u01f1\7\uffff\6\157\32\uffff\6\157",
            "\1\157"
    };

    static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
    static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
    static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
    static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
    static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
    static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
    static final short[][] DFA19_transition;

    static {
        int numStates = DFA19_transitionS.length;
        DFA19_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
        }
    }

    class DFA19 extends DFA {

        public DFA19(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 19;
            this.eot = DFA19_eot;
            this.eof = DFA19_eof;
            this.min = DFA19_min;
            this.max = DFA19_max;
            this.accept = DFA19_accept;
            this.special = DFA19_special;
            this.transition = DFA19_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__125 | T__126 | T__127 | T__128 | T__129 | T__130 | T__131 | T__132 | T__133 | T__134 | T__135 | T__136 | T__137 | T__138 | T__139 | T__140 | T__141 | T__142 | K_SELECT | K_FROM | K_WHERE | K_AND | K_KEY | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_USE | K_COUNT | K_SET | K_BEGIN | K_UNLOGGED | K_BATCH | K_APPLY | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_KEYSPACES | K_COLUMNFAMILY | K_INDEX | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_RENAME | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_ALLOW | K_FILTERING | K_GRANT | K_ALL | K_PERMISSION | K_PERMISSIONS | K_OF | K_REVOKE | K_MODIFY | K_AUTHORIZE | K_NORECURSIVE | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | K_MAP | K_LIST | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | BOOLEAN | IDENT | HEXNUMBER | UUID | WS | COMMENT | MULTILINE_COMMENT );";
        }
    }
 

}