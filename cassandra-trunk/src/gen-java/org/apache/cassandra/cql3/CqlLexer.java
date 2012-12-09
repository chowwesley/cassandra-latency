// $ANTLR 3.2 Sep 23, 2009 12:02:23 /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g 2012-10-19 20:50:37

    package org.apache.cassandra.cql3;

    import org.apache.cassandra.thrift.InvalidRequestException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlLexer extends Lexer {
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
    public static final int K_TRUNCATE=47;
    public static final int COMMENT=110;
    public static final int K_FULL_ACCESS=57;
    public static final int K_ORDER=12;
    public static final int K_ALL=99;
    public static final int D=92;
    public static final int E=80;
    public static final int F=84;
    public static final int G=98;
    public static final int K_TYPE=44;
    public static final int K_KEYSPACE=32;
    public static final int K_COUNT=6;
    public static final int A=90;
    public static final int B=101;
    public static final int C=82;
    public static final int L=81;
    public static final int M=87;
    public static final int N=91;
    public static final int O=86;
    public static final int H=89;
    public static final int I=95;
    public static final int K_UPDATE=25;
    public static final int J=104;
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
    public static final int K_VARINT=77;
    public static final int K_FLOAT=72;
    public static final int K_DOUBLE=71;
    public static final int K_SELECT=5;
    public static final int K_LIMIT=14;
    public static final int K_BOOLEAN=68;
    public static final int K_ALTER=43;
    public static final int K_SET=26;
    public static final int K_WHERE=11;
    public static final int QUOTED_NAME=59;
    public static final int K_DESCRIBE=56;
    public static final int MULTILINE_COMMENT=111;
    public static final int K_BLOB=67;
    public static final int K_INTO=22;
    public static final int HEX=108;
    public static final int K_REVOKE=52;
    public static final int K_VARCHAR=76;
    public static final int IDENT=41;
    public static final int T__113=113;
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

        public void throwLastRecognitionError() throws InvalidRequestException
        {
            if (recognitionErrors.size() > 0)
                throw new InvalidRequestException(recognitionErrors.get((recognitionErrors.size()-1)));
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
    public String getGrammarFileName() { return "/Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g"; }

    // $ANTLR start "T__112"
    public final void mT__112() throws RecognitionException {
        try {
            int _type = T__112;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:50:8: ( ';' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:50:10: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__112"

    // $ANTLR start "T__113"
    public final void mT__113() throws RecognitionException {
        try {
            int _type = T__113;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:51:8: ( '(' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:51:10: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__113"

    // $ANTLR start "T__114"
    public final void mT__114() throws RecognitionException {
        try {
            int _type = T__114;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:52:8: ( ')' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:52:10: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__114"

    // $ANTLR start "T__115"
    public final void mT__115() throws RecognitionException {
        try {
            int _type = T__115;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:53:8: ( ',' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:53:10: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__115"

    // $ANTLR start "T__116"
    public final void mT__116() throws RecognitionException {
        try {
            int _type = T__116;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:54:8: ( '\\*' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:54:10: '\\*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__116"

    // $ANTLR start "T__117"
    public final void mT__117() throws RecognitionException {
        try {
            int _type = T__117;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:55:8: ( '=' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:55:10: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__117"

    // $ANTLR start "T__118"
    public final void mT__118() throws RecognitionException {
        try {
            int _type = T__118;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:56:8: ( '.' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:56:10: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__118"

    // $ANTLR start "T__119"
    public final void mT__119() throws RecognitionException {
        try {
            int _type = T__119;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:57:8: ( '+' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:57:10: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__119"

    // $ANTLR start "T__120"
    public final void mT__120() throws RecognitionException {
        try {
            int _type = T__120;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:58:8: ( '-' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:58:10: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__120"

    // $ANTLR start "T__121"
    public final void mT__121() throws RecognitionException {
        try {
            int _type = T__121;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:59:8: ( ':' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:59:10: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__121"

    // $ANTLR start "T__122"
    public final void mT__122() throws RecognitionException {
        try {
            int _type = T__122;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:60:8: ( '<' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:60:10: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__122"

    // $ANTLR start "T__123"
    public final void mT__123() throws RecognitionException {
        try {
            int _type = T__123;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:61:8: ( '<=' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:61:10: '<='
            {
            match("<="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__123"

    // $ANTLR start "T__124"
    public final void mT__124() throws RecognitionException {
        try {
            int _type = T__124;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:62:8: ( '>=' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:62:10: '>='
            {
            match(">="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__124"

    // $ANTLR start "T__125"
    public final void mT__125() throws RecognitionException {
        try {
            int _type = T__125;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:63:8: ( '>' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:63:10: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__125"

    // $ANTLR start "K_SELECT"
    public final void mK_SELECT() throws RecognitionException {
        try {
            int _type = K_SELECT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:627:9: ( S E L E C T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:627:16: S E L E C T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:628:7: ( F R O M )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:628:16: F R O M
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:629:8: ( W H E R E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:629:16: W H E R E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:630:6: ( A N D )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:630:16: A N D
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:631:6: ( K E Y )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:631:16: K E Y
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:632:9: ( I N S E R T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:632:16: I N S E R T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:633:9: ( U P D A T E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:633:16: U P D A T E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:634:7: ( W I T H )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:634:16: W I T H
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:635:8: ( L I M I T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:635:16: L I M I T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:636:8: ( U S I N G )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:636:16: U S I N G
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

    // $ANTLR start "K_ALL"
    public final void mK_ALL() throws RecognitionException {
        try {
            int _type = K_ALL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:637:6: ( A L L )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:637:16: A L L
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

    // $ANTLR start "K_CONSISTENCY"
    public final void mK_CONSISTENCY() throws RecognitionException {
        try {
            int _type = K_CONSISTENCY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:638:14: ( C O N S I S T E N C Y )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:638:16: C O N S I S T E N C Y
            {
            mC(); 
            mO(); 
            mN(); 
            mS(); 
            mI(); 
            mS(); 
            mT(); 
            mE(); 
            mN(); 
            mC(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_CONSISTENCY"

    // $ANTLR start "K_LEVEL"
    public final void mK_LEVEL() throws RecognitionException {
        try {
            int _type = K_LEVEL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:639:8: ( ( O N E | Q U O R U M | K_ALL | A N Y | L O C A L '_' Q U O R U M | E A C H '_' Q U O R U M | T W O | T H R E E ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:639:16: ( O N E | Q U O R U M | K_ALL | A N Y | L O C A L '_' Q U O R U M | E A C H '_' Q U O R U M | T W O | T H R E E )
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:639:16: ( O N E | Q U O R U M | K_ALL | A N Y | L O C A L '_' Q U O R U M | E A C H '_' Q U O R U M | T W O | T H R E E )
            int alt1=8;
            alt1 = dfa1.predict(input);
            switch (alt1) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:639:18: O N E
                    {
                    mO(); 
                    mN(); 
                    mE(); 

                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:640:18: Q U O R U M
                    {
                    mQ(); 
                    mU(); 
                    mO(); 
                    mR(); 
                    mU(); 
                    mM(); 

                    }
                    break;
                case 3 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:641:18: K_ALL
                    {
                    mK_ALL(); 

                    }
                    break;
                case 4 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:642:18: A N Y
                    {
                    mA(); 
                    mN(); 
                    mY(); 

                    }
                    break;
                case 5 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:643:18: L O C A L '_' Q U O R U M
                    {
                    mL(); 
                    mO(); 
                    mC(); 
                    mA(); 
                    mL(); 
                    match('_'); 
                    mQ(); 
                    mU(); 
                    mO(); 
                    mR(); 
                    mU(); 
                    mM(); 

                    }
                    break;
                case 6 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:644:18: E A C H '_' Q U O R U M
                    {
                    mE(); 
                    mA(); 
                    mC(); 
                    mH(); 
                    match('_'); 
                    mQ(); 
                    mU(); 
                    mO(); 
                    mR(); 
                    mU(); 
                    mM(); 

                    }
                    break;
                case 7 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:645:18: T W O
                    {
                    mT(); 
                    mW(); 
                    mO(); 

                    }
                    break;
                case 8 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:646:18: T H R E E
                    {
                    mT(); 
                    mH(); 
                    mR(); 
                    mE(); 
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
    // $ANTLR end "K_LEVEL"

    // $ANTLR start "K_USE"
    public final void mK_USE() throws RecognitionException {
        try {
            int _type = K_USE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:649:6: ( U S E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:649:16: U S E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:650:8: ( C O U N T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:650:16: C O U N T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:651:6: ( S E T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:651:16: S E T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:652:8: ( B E G I N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:652:16: B E G I N
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

    // $ANTLR start "K_APPLY"
    public final void mK_APPLY() throws RecognitionException {
        try {
            int _type = K_APPLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:653:8: ( A P P L Y )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:653:16: A P P L Y
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

    // $ANTLR start "K_BATCH"
    public final void mK_BATCH() throws RecognitionException {
        try {
            int _type = K_BATCH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:654:8: ( B A T C H )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:654:16: B A T C H
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

    // $ANTLR start "K_TRUNCATE"
    public final void mK_TRUNCATE() throws RecognitionException {
        try {
            int _type = K_TRUNCATE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:655:11: ( T R U N C A T E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:655:16: T R U N C A T E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:9: ( D E L E T E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:656:16: D E L E T E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:657:5: ( I N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:657:16: I N
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:658:9: ( C R E A T E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:658:16: C R E A T E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:659:11: ( ( K E Y S P A C E | S C H E M A ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:659:16: ( K E Y S P A C E | S C H E M A )
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:659:16: ( K E Y S P A C E | S C H E M A )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='K'||LA2_0=='k') ) {
                alt2=1;
            }
            else if ( (LA2_0=='S'||LA2_0=='s') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:659:18: K E Y S P A C E
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
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:660:20: S C H E M A
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

    // $ANTLR start "K_COLUMNFAMILY"
    public final void mK_COLUMNFAMILY() throws RecognitionException {
        try {
            int _type = K_COLUMNFAMILY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:661:15: ( ( C O L U M N F A M I L Y | T A B L E ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:661:16: ( C O L U M N F A M I L Y | T A B L E )
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:661:16: ( C O L U M N F A M I L Y | T A B L E )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='C'||LA3_0=='c') ) {
                alt3=1;
            }
            else if ( (LA3_0=='T'||LA3_0=='t') ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:661:18: C O L U M N F A M I L Y
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
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:662:20: T A B L E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:663:8: ( I N D E X )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:663:16: I N D E X
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:664:5: ( O N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:664:16: O N
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:665:5: ( T O )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:665:16: T O
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:666:7: ( D R O P )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:666:16: D R O P
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:667:10: ( P R I M A R Y )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:667:16: P R I M A R Y
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:668:7: ( I N T O )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:668:16: I N T O
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:669:9: ( V A L U E S )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:669:16: V A L U E S
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:670:12: ( T I M E S T A M P )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:670:16: T I M E S T A M P
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:671:6: ( T T L )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:671:16: T T L
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:672:8: ( A L T E R )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:672:16: A L T E R
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

    // $ANTLR start "K_ADD"
    public final void mK_ADD() throws RecognitionException {
        try {
            int _type = K_ADD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:673:6: ( A D D )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:673:16: A D D
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:674:7: ( T Y P E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:674:16: T Y P E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:675:10: ( C O M P A C T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:675:16: C O M P A C T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:676:10: ( S T O R A G E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:676:16: S T O R A G E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:677:8: ( O R D E R )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:677:16: O R D E R
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:678:5: ( B Y )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:678:16: B Y
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:679:6: ( A S C )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:679:16: A S C
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:680:7: ( D E S C )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:680:16: D E S C
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

    // $ANTLR start "K_GRANT"
    public final void mK_GRANT() throws RecognitionException {
        try {
            int _type = K_GRANT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:681:8: ( G R A N T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:681:16: G R A N T
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

    // $ANTLR start "K_GRANTS"
    public final void mK_GRANTS() throws RecognitionException {
        try {
            int _type = K_GRANTS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:682:9: ( G R A N T S )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:682:16: G R A N T S
            {
            mG(); 
            mR(); 
            mA(); 
            mN(); 
            mT(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_GRANTS"

    // $ANTLR start "K_REVOKE"
    public final void mK_REVOKE() throws RecognitionException {
        try {
            int _type = K_REVOKE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:683:9: ( R E V O K E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:683:16: R E V O K E
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

    // $ANTLR start "K_OPTION"
    public final void mK_OPTION() throws RecognitionException {
        try {
            int _type = K_OPTION;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:684:9: ( O P T I O N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:684:16: O P T I O N
            {
            mO(); 
            mP(); 
            mT(); 
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
    // $ANTLR end "K_OPTION"

    // $ANTLR start "K_DESCRIBE"
    public final void mK_DESCRIBE() throws RecognitionException {
        try {
            int _type = K_DESCRIBE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:685:11: ( D E S C R I B E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:685:16: D E S C R I B E
            {
            mD(); 
            mE(); 
            mS(); 
            mC(); 
            mR(); 
            mI(); 
            mB(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_DESCRIBE"

    // $ANTLR start "K_FOR"
    public final void mK_FOR() throws RecognitionException {
        try {
            int _type = K_FOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:686:6: ( F O R )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:686:16: F O R
            {
            mF(); 
            mO(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_FOR"

    // $ANTLR start "K_LIST"
    public final void mK_LIST() throws RecognitionException {
        try {
            int _type = K_LIST;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:687:7: ( L I S T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:687:16: L I S T
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

    // $ANTLR start "K_FULL_ACCESS"
    public final void mK_FULL_ACCESS() throws RecognitionException {
        try {
            int _type = K_FULL_ACCESS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:688:14: ( F U L L '_' A C C E S S )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:688:16: F U L L '_' A C C E S S
            {
            mF(); 
            mU(); 
            mL(); 
            mL(); 
            match('_'); 
            mA(); 
            mC(); 
            mC(); 
            mE(); 
            mS(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_FULL_ACCESS"

    // $ANTLR start "K_NO_ACCESS"
    public final void mK_NO_ACCESS() throws RecognitionException {
        try {
            int _type = K_NO_ACCESS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:689:12: ( N O '_' A C C E S S )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:689:16: N O '_' A C C E S S
            {
            mN(); 
            mO(); 
            match('_'); 
            mA(); 
            mC(); 
            mC(); 
            mE(); 
            mS(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "K_NO_ACCESS"

    // $ANTLR start "K_CLUSTERING"
    public final void mK_CLUSTERING() throws RecognitionException {
        try {
            int _type = K_CLUSTERING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:692:13: ( C L U S T E R I N G )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:692:16: C L U S T E R I N G
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:693:8: ( A S C I I )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:693:16: A S C I I
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:694:9: ( B I G I N T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:694:16: B I G I N T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:695:7: ( B L O B )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:695:16: B L O B
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:696:10: ( B O O L E A N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:696:16: B O O L E A N
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:697:10: ( C O U N T E R )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:697:16: C O U N T E R
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:10: ( D E C I M A L )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:698:16: D E C I M A L
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:699:9: ( D O U B L E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:699:16: D O U B L E
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:700:8: ( F L O A T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:700:16: F L O A T
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

    // $ANTLR start "K_INT"
    public final void mK_INT() throws RecognitionException {
        try {
            int _type = K_INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:701:6: ( I N T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:701:16: I N T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:702:7: ( T E X T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:702:16: T E X T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:703:7: ( U U I D )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:703:16: U U I D
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:704:10: ( V A R C H A R )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:704:16: V A R C H A R
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:705:9: ( V A R I N T )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:705:16: V A R I N T
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:706:11: ( T I M E U U I D )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:706:16: T I M E U U I D
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:707:8: ( T O K E N )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:707:16: T O K E N
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:708:12: ( W R I T E T I M E )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:708:16: W R I T E T I M E
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

    // $ANTLR start "A"
    public final void mA() throws RecognitionException {
        try {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:711:11: ( ( 'a' | 'A' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:711:13: ( 'a' | 'A' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:712:11: ( ( 'b' | 'B' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:712:13: ( 'b' | 'B' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:713:11: ( ( 'c' | 'C' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:713:13: ( 'c' | 'C' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:714:11: ( ( 'd' | 'D' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:714:13: ( 'd' | 'D' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:715:11: ( ( 'e' | 'E' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:715:13: ( 'e' | 'E' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:716:11: ( ( 'f' | 'F' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:716:13: ( 'f' | 'F' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:717:11: ( ( 'g' | 'G' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:717:13: ( 'g' | 'G' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:718:11: ( ( 'h' | 'H' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:718:13: ( 'h' | 'H' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:719:11: ( ( 'i' | 'I' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:719:13: ( 'i' | 'I' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:720:11: ( ( 'j' | 'J' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:720:13: ( 'j' | 'J' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:721:11: ( ( 'k' | 'K' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:721:13: ( 'k' | 'K' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:722:11: ( ( 'l' | 'L' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:722:13: ( 'l' | 'L' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:723:11: ( ( 'm' | 'M' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:723:13: ( 'm' | 'M' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:724:11: ( ( 'n' | 'N' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:724:13: ( 'n' | 'N' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:725:11: ( ( 'o' | 'O' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:725:13: ( 'o' | 'O' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:726:11: ( ( 'p' | 'P' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:726:13: ( 'p' | 'P' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:727:11: ( ( 'q' | 'Q' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:727:13: ( 'q' | 'Q' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:728:11: ( ( 'r' | 'R' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:728:13: ( 'r' | 'R' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:729:11: ( ( 's' | 'S' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:729:13: ( 's' | 'S' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:11: ( ( 't' | 'T' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:730:13: ( 't' | 'T' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:731:11: ( ( 'u' | 'U' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:731:13: ( 'u' | 'U' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:732:11: ( ( 'v' | 'V' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:732:13: ( 'v' | 'V' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:733:11: ( ( 'w' | 'W' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:733:13: ( 'w' | 'W' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:734:11: ( ( 'x' | 'X' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:734:13: ( 'x' | 'X' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:735:11: ( ( 'y' | 'Y' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:735:13: ( 'y' | 'Y' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:736:11: ( ( 'z' | 'Z' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:736:13: ( 'z' | 'Z' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:741:5: ( '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\'' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:741:7: '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\''
            {
            match('\''); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:741:12: (c=~ ( '\\'' ) | '\\'' '\\'' )*
            loop4:
            do {
                int alt4=3;
                int LA4_0 = input.LA(1);

                if ( (LA4_0=='\'') ) {
                    int LA4_1 = input.LA(2);

                    if ( (LA4_1=='\'') ) {
                        alt4=2;
                    }


                }
                else if ( ((LA4_0>='\u0000' && LA4_0<='&')||(LA4_0>='(' && LA4_0<='\uFFFF')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:741:13: c=~ ( '\\'' )
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
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:741:50: '\\'' '\\''
            	    {
            	    match('\''); 
            	    match('\''); 
            	     b.appendCodePoint('\''); 

            	    }
            	    break;

            	default :
            	    break loop4;
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:747:5: ( '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )* '\\\"' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:747:7: '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )* '\\\"'
            {
            match('\"'); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:747:12: (c=~ ( '\\\"' ) | '\\\"' '\\\"' )*
            loop5:
            do {
                int alt5=3;
                int LA5_0 = input.LA(1);

                if ( (LA5_0=='\"') ) {
                    int LA5_1 = input.LA(2);

                    if ( (LA5_1=='\"') ) {
                        alt5=2;
                    }


                }
                else if ( ((LA5_0>='\u0000' && LA5_0<='!')||(LA5_0>='#' && LA5_0<='\uFFFF')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:747:13: c=~ ( '\\\"' )
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
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:747:51: '\\\"' '\\\"'
            	    {
            	    match('\"'); 
            	    match('\"'); 
            	     b.appendCodePoint('\"'); 

            	    }
            	    break;

            	default :
            	    break loop5;
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:751:5: ( '0' .. '9' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:751:7: '0' .. '9'
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:755:5: ( ( 'A' .. 'Z' | 'a' .. 'z' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:755:7: ( 'A' .. 'Z' | 'a' .. 'z' )
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:759:5: ( ( 'A' .. 'F' | 'a' .. 'f' | '0' .. '9' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:759:7: ( 'A' .. 'F' | 'a' .. 'f' | '0' .. '9' )
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

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:5: ( ( '-' )? ( DIGIT )+ )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:7: ( '-' )? ( DIGIT )+
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:7: ( '-' )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0=='-') ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:7: '-'
                    {
                    match('-'); 

                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:12: ( DIGIT )+
            int cnt7=0;
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( ((LA7_0>='0' && LA7_0<='9')) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:763:12: DIGIT
            	    {
            	    mDIGIT(); 

            	    }
            	    break;

            	default :
            	    if ( cnt7 >= 1 ) break loop7;
                        EarlyExitException eee =
                            new EarlyExitException(7, input);
                        throw eee;
                }
                cnt7++;
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:767:5: ( '?' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:767:7: '?'
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:775:5: ( INTEGER '.' ( DIGIT )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:775:7: INTEGER '.' ( DIGIT )*
            {
            mINTEGER(); 
            match('.'); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:775:19: ( DIGIT )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( ((LA8_0>='0' && LA8_0<='9')) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:775:19: DIGIT
            	    {
            	    mDIGIT(); 

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FLOAT"

    // $ANTLR start "IDENT"
    public final void mIDENT() throws RecognitionException {
        try {
            int _type = IDENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:779:5: ( LETTER ( LETTER | DIGIT | '_' )* )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:779:7: LETTER ( LETTER | DIGIT | '_' )*
            {
            mLETTER(); 
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:779:14: ( LETTER | DIGIT | '_' )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( ((LA9_0>='0' && LA9_0<='9')||(LA9_0>='A' && LA9_0<='Z')||LA9_0=='_'||(LA9_0>='a' && LA9_0<='z')) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
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
            	    break loop9;
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

    // $ANTLR start "UUID"
    public final void mUUID() throws RecognitionException {
        try {
            int _type = UUID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:783:5: ( HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:783:7: HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:791:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:791:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:791:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            int cnt10=0;
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( ((LA10_0>='\t' && LA10_0<='\n')||LA10_0=='\r'||LA10_0==' ') ) {
                    alt10=1;
                }


                switch (alt10) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:
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
            	    if ( cnt10 >= 1 ) break loop10;
                        EarlyExitException eee =
                            new EarlyExitException(10, input);
                        throw eee;
                }
                cnt10++;
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:5: ( ( '--' | '//' ) ( . )* ( '\\n' | '\\r' ) )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:7: ( '--' | '//' ) ( . )* ( '\\n' | '\\r' )
            {
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:7: ( '--' | '//' )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0=='-') ) {
                alt11=1;
            }
            else if ( (LA11_0=='/') ) {
                alt11=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:8: '--'
                    {
                    match("--"); 


                    }
                    break;
                case 2 :
                    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:15: '//'
                    {
                    match("//"); 


                    }
                    break;

            }

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:21: ( . )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0=='\n'||LA12_0=='\r') ) {
                    alt12=2;
                }
                else if ( ((LA12_0>='\u0000' && LA12_0<='\t')||(LA12_0>='\u000B' && LA12_0<='\f')||(LA12_0>='\u000E' && LA12_0<='\uFFFF')) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:795:21: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop12;
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
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:799:5: ( '/*' ( . )* '*/' )
            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:799:7: '/*' ( . )* '*/'
            {
            match("/*"); 

            // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:799:12: ( . )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0=='*') ) {
                    int LA13_1 = input.LA(2);

                    if ( (LA13_1=='/') ) {
                        alt13=2;
                    }
                    else if ( ((LA13_1>='\u0000' && LA13_1<='.')||(LA13_1>='0' && LA13_1<='\uFFFF')) ) {
                        alt13=1;
                    }


                }
                else if ( ((LA13_0>='\u0000' && LA13_0<=')')||(LA13_0>='+' && LA13_0<='\uFFFF')) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:799:12: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop13;
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
        // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:8: ( T__112 | T__113 | T__114 | T__115 | T__116 | T__117 | T__118 | T__119 | T__120 | T__121 | T__122 | T__123 | T__124 | T__125 | K_SELECT | K_FROM | K_WHERE | K_AND | K_KEY | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_ALL | K_CONSISTENCY | K_LEVEL | K_USE | K_COUNT | K_SET | K_BEGIN | K_APPLY | K_BATCH | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_COLUMNFAMILY | K_INDEX | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_GRANT | K_GRANTS | K_REVOKE | K_OPTION | K_DESCRIBE | K_FOR | K_LIST | K_FULL_ACCESS | K_NO_ACCESS | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | IDENT | UUID | WS | COMMENT | MULTILINE_COMMENT )
        int alt14=93;
        alt14 = dfa14.predict(input);
        switch (alt14) {
            case 1 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:10: T__112
                {
                mT__112(); 

                }
                break;
            case 2 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:17: T__113
                {
                mT__113(); 

                }
                break;
            case 3 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:24: T__114
                {
                mT__114(); 

                }
                break;
            case 4 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:31: T__115
                {
                mT__115(); 

                }
                break;
            case 5 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:38: T__116
                {
                mT__116(); 

                }
                break;
            case 6 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:45: T__117
                {
                mT__117(); 

                }
                break;
            case 7 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:52: T__118
                {
                mT__118(); 

                }
                break;
            case 8 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:59: T__119
                {
                mT__119(); 

                }
                break;
            case 9 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:66: T__120
                {
                mT__120(); 

                }
                break;
            case 10 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:73: T__121
                {
                mT__121(); 

                }
                break;
            case 11 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:80: T__122
                {
                mT__122(); 

                }
                break;
            case 12 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:87: T__123
                {
                mT__123(); 

                }
                break;
            case 13 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:94: T__124
                {
                mT__124(); 

                }
                break;
            case 14 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:101: T__125
                {
                mT__125(); 

                }
                break;
            case 15 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:108: K_SELECT
                {
                mK_SELECT(); 

                }
                break;
            case 16 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:117: K_FROM
                {
                mK_FROM(); 

                }
                break;
            case 17 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:124: K_WHERE
                {
                mK_WHERE(); 

                }
                break;
            case 18 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:132: K_AND
                {
                mK_AND(); 

                }
                break;
            case 19 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:138: K_KEY
                {
                mK_KEY(); 

                }
                break;
            case 20 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:144: K_INSERT
                {
                mK_INSERT(); 

                }
                break;
            case 21 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:153: K_UPDATE
                {
                mK_UPDATE(); 

                }
                break;
            case 22 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:162: K_WITH
                {
                mK_WITH(); 

                }
                break;
            case 23 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:169: K_LIMIT
                {
                mK_LIMIT(); 

                }
                break;
            case 24 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:177: K_USING
                {
                mK_USING(); 

                }
                break;
            case 25 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:185: K_ALL
                {
                mK_ALL(); 

                }
                break;
            case 26 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:191: K_CONSISTENCY
                {
                mK_CONSISTENCY(); 

                }
                break;
            case 27 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:205: K_LEVEL
                {
                mK_LEVEL(); 

                }
                break;
            case 28 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:213: K_USE
                {
                mK_USE(); 

                }
                break;
            case 29 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:219: K_COUNT
                {
                mK_COUNT(); 

                }
                break;
            case 30 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:227: K_SET
                {
                mK_SET(); 

                }
                break;
            case 31 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:233: K_BEGIN
                {
                mK_BEGIN(); 

                }
                break;
            case 32 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:241: K_APPLY
                {
                mK_APPLY(); 

                }
                break;
            case 33 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:249: K_BATCH
                {
                mK_BATCH(); 

                }
                break;
            case 34 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:257: K_TRUNCATE
                {
                mK_TRUNCATE(); 

                }
                break;
            case 35 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:268: K_DELETE
                {
                mK_DELETE(); 

                }
                break;
            case 36 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:277: K_IN
                {
                mK_IN(); 

                }
                break;
            case 37 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:282: K_CREATE
                {
                mK_CREATE(); 

                }
                break;
            case 38 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:291: K_KEYSPACE
                {
                mK_KEYSPACE(); 

                }
                break;
            case 39 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:302: K_COLUMNFAMILY
                {
                mK_COLUMNFAMILY(); 

                }
                break;
            case 40 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:317: K_INDEX
                {
                mK_INDEX(); 

                }
                break;
            case 41 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:325: K_ON
                {
                mK_ON(); 

                }
                break;
            case 42 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:330: K_TO
                {
                mK_TO(); 

                }
                break;
            case 43 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:335: K_DROP
                {
                mK_DROP(); 

                }
                break;
            case 44 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:342: K_PRIMARY
                {
                mK_PRIMARY(); 

                }
                break;
            case 45 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:352: K_INTO
                {
                mK_INTO(); 

                }
                break;
            case 46 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:359: K_VALUES
                {
                mK_VALUES(); 

                }
                break;
            case 47 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:368: K_TIMESTAMP
                {
                mK_TIMESTAMP(); 

                }
                break;
            case 48 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:380: K_TTL
                {
                mK_TTL(); 

                }
                break;
            case 49 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:386: K_ALTER
                {
                mK_ALTER(); 

                }
                break;
            case 50 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:394: K_ADD
                {
                mK_ADD(); 

                }
                break;
            case 51 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:400: K_TYPE
                {
                mK_TYPE(); 

                }
                break;
            case 52 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:407: K_COMPACT
                {
                mK_COMPACT(); 

                }
                break;
            case 53 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:417: K_STORAGE
                {
                mK_STORAGE(); 

                }
                break;
            case 54 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:427: K_ORDER
                {
                mK_ORDER(); 

                }
                break;
            case 55 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:435: K_BY
                {
                mK_BY(); 

                }
                break;
            case 56 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:440: K_ASC
                {
                mK_ASC(); 

                }
                break;
            case 57 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:446: K_DESC
                {
                mK_DESC(); 

                }
                break;
            case 58 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:453: K_GRANT
                {
                mK_GRANT(); 

                }
                break;
            case 59 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:461: K_GRANTS
                {
                mK_GRANTS(); 

                }
                break;
            case 60 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:470: K_REVOKE
                {
                mK_REVOKE(); 

                }
                break;
            case 61 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:479: K_OPTION
                {
                mK_OPTION(); 

                }
                break;
            case 62 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:488: K_DESCRIBE
                {
                mK_DESCRIBE(); 

                }
                break;
            case 63 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:499: K_FOR
                {
                mK_FOR(); 

                }
                break;
            case 64 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:505: K_LIST
                {
                mK_LIST(); 

                }
                break;
            case 65 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:512: K_FULL_ACCESS
                {
                mK_FULL_ACCESS(); 

                }
                break;
            case 66 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:526: K_NO_ACCESS
                {
                mK_NO_ACCESS(); 

                }
                break;
            case 67 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:538: K_CLUSTERING
                {
                mK_CLUSTERING(); 

                }
                break;
            case 68 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:551: K_ASCII
                {
                mK_ASCII(); 

                }
                break;
            case 69 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:559: K_BIGINT
                {
                mK_BIGINT(); 

                }
                break;
            case 70 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:568: K_BLOB
                {
                mK_BLOB(); 

                }
                break;
            case 71 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:575: K_BOOLEAN
                {
                mK_BOOLEAN(); 

                }
                break;
            case 72 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:585: K_COUNTER
                {
                mK_COUNTER(); 

                }
                break;
            case 73 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:595: K_DECIMAL
                {
                mK_DECIMAL(); 

                }
                break;
            case 74 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:605: K_DOUBLE
                {
                mK_DOUBLE(); 

                }
                break;
            case 75 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:614: K_FLOAT
                {
                mK_FLOAT(); 

                }
                break;
            case 76 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:622: K_INT
                {
                mK_INT(); 

                }
                break;
            case 77 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:628: K_TEXT
                {
                mK_TEXT(); 

                }
                break;
            case 78 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:635: K_UUID
                {
                mK_UUID(); 

                }
                break;
            case 79 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:642: K_VARCHAR
                {
                mK_VARCHAR(); 

                }
                break;
            case 80 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:652: K_VARINT
                {
                mK_VARINT(); 

                }
                break;
            case 81 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:661: K_TIMEUUID
                {
                mK_TIMEUUID(); 

                }
                break;
            case 82 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:672: K_TOKEN
                {
                mK_TOKEN(); 

                }
                break;
            case 83 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:680: K_WRITETIME
                {
                mK_WRITETIME(); 

                }
                break;
            case 84 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:692: STRING_LITERAL
                {
                mSTRING_LITERAL(); 

                }
                break;
            case 85 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:707: QUOTED_NAME
                {
                mQUOTED_NAME(); 

                }
                break;
            case 86 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:719: INTEGER
                {
                mINTEGER(); 

                }
                break;
            case 87 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:727: QMARK
                {
                mQMARK(); 

                }
                break;
            case 88 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:733: FLOAT
                {
                mFLOAT(); 

                }
                break;
            case 89 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:739: IDENT
                {
                mIDENT(); 

                }
                break;
            case 90 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:745: UUID
                {
                mUUID(); 

                }
                break;
            case 91 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:750: WS
                {
                mWS(); 

                }
                break;
            case 92 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:753: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 93 :
                // /Users/WesleyChow/Documents/Berkeley/Fall2012/CS262A/Project/cassandra-latency/cassandra-trunk/src/java/org/apache/cassandra/cql3/Cql.g:1:761: MULTILINE_COMMENT
                {
                mMULTILINE_COMMENT(); 

                }
                break;

        }

    }


    protected DFA1 dfa1 = new DFA1(this);
    protected DFA14 dfa14 = new DFA14(this);
    static final String DFA1_eotS =
        "\13\uffff";
    static final String DFA1_eofS =
        "\13\uffff";
    static final String DFA1_minS =
        "\1\101\2\uffff\1\114\2\uffff\1\110\4\uffff";
    static final String DFA1_maxS =
        "\1\164\2\uffff\1\156\2\uffff\1\167\4\uffff";
    static final String DFA1_acceptS =
        "\1\uffff\1\1\1\2\1\uffff\1\5\1\6\1\uffff\1\3\1\4\1\10\1\7";
    static final String DFA1_specialS =
        "\13\uffff}>";
    static final String[] DFA1_transitionS = {
            "\1\3\3\uffff\1\5\6\uffff\1\4\2\uffff\1\1\1\uffff\1\2\2\uffff"+
            "\1\6\14\uffff\1\3\3\uffff\1\5\6\uffff\1\4\2\uffff\1\1\1\uffff"+
            "\1\2\2\uffff\1\6",
            "",
            "",
            "\1\7\1\uffff\1\10\35\uffff\1\7\1\uffff\1\10",
            "",
            "",
            "\1\11\16\uffff\1\12\20\uffff\1\11\16\uffff\1\12",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA1_eot = DFA.unpackEncodedString(DFA1_eotS);
    static final short[] DFA1_eof = DFA.unpackEncodedString(DFA1_eofS);
    static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars(DFA1_minS);
    static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars(DFA1_maxS);
    static final short[] DFA1_accept = DFA.unpackEncodedString(DFA1_acceptS);
    static final short[] DFA1_special = DFA.unpackEncodedString(DFA1_specialS);
    static final short[][] DFA1_transition;

    static {
        int numStates = DFA1_transitionS.length;
        DFA1_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA1_transition[i] = DFA.unpackEncodedString(DFA1_transitionS[i]);
        }
    }

    class DFA1 extends DFA {

        public DFA1(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 1;
            this.eot = DFA1_eot;
            this.eof = DFA1_eof;
            this.min = DFA1_min;
            this.max = DFA1_max;
            this.accept = DFA1_accept;
            this.special = DFA1_special;
            this.transition = DFA1_transition;
        }
        public String getDescription() {
            return "639:16: ( O N E | Q U O R U M | K_ALL | A N Y | L O C A L '_' Q U O R U M | E A C H '_' Q U O R U M | T W O | T H R E E )";
        }
    }
    static final String DFA14_eotS =
        "\11\uffff\1\52\1\uffff\1\54\1\56\24\45\2\uffff\1\145\5\uffff\1\145"+
        "\5\uffff\21\45\1\176\11\45\1\u0090\7\45\1\u0099\5\45\1\u00a0\14"+
        "\45\1\uffff\1\145\3\uffff\1\45\1\u00b2\6\45\1\u00b9\3\45\1\u00bd"+
        "\1\u00be\1\45\1\u00c0\1\u00c1\1\u00c2\1\45\1\u00c5\1\uffff\1\45"+
        "\1\u00c8\2\45\1\u00cc\14\45\1\uffff\1\u00c1\6\45\1\u00c1\1\uffff"+
        "\2\45\1\u00e1\3\45\1\uffff\17\45\1\145\1\45\1\uffff\5\45\1\u00fc"+
        "\1\uffff\1\u00fd\2\45\2\uffff\1\45\3\uffff\2\45\1\uffff\2\45\1\uffff"+
        "\1\u0105\2\45\1\uffff\1\u0108\2\45\1\u010b\15\45\1\u0119\1\45\1"+
        "\u011b\1\uffff\4\45\1\u0121\3\45\1\u0125\1\45\1\u0128\10\45\1\145"+
        "\4\45\1\u0136\1\45\2\uffff\1\45\1\u0139\1\u013a\1\u013b\1\u013c"+
        "\1\45\1\u013e\1\uffff\1\45\1\u0140\1\uffff\1\45\1\u0142\1\uffff"+
        "\2\45\1\u0145\4\45\1\u014b\3\45\1\u014f\1\45\1\uffff\1\u0151\1\uffff"+
        "\2\45\1\u00c1\1\u0154\1\45\1\uffff\1\45\1\u0157\1\45\1\uffff\2\45"+
        "\1\uffff\5\45\1\u0160\2\45\1\145\1\u0165\1\u0166\2\45\1\uffff\2"+
        "\45\4\uffff\1\45\1\uffff\1\u016c\1\uffff\1\u016d\1\uffff\1\45\1"+
        "\u016f\1\uffff\5\45\1\uffff\1\u0175\1\u00c1\1\45\1\uffff\1\45\1"+
        "\uffff\2\45\1\uffff\1\u017a\1\45\1\uffff\1\u017c\2\45\1\u017f\2"+
        "\45\1\u0182\1\u0183\1\uffff\1\u0184\1\u0185\1\45\1\145\2\uffff\1"+
        "\u0188\4\45\2\uffff\1\45\1\uffff\1\u018e\1\u018f\3\45\1\uffff\4"+
        "\45\1\uffff\1\u0197\1\uffff\1\45\1\u0199\1\uffff\1\u019a\1\u019b"+
        "\4\uffff\1\45\1\145\1\uffff\3\45\1\u0166\1\45\2\uffff\4\45\1\u01a5"+
        "\1\u01a6\1\45\1\uffff\1\u01a8\3\uffff\1\45\1\145\1\45\1\u01ab\5"+
        "\45\2\uffff\1\u01b1\1\uffff\1\u01b2\1\45\1\uffff\3\45\1\u01b7\1"+
        "\45\2\uffff\1\u01b9\1\45\1\u01bb\1\45\1\uffff\1\u00c1\1\uffff\1"+
        "\u00c1\1\uffff\1\u014f";
    static final String DFA14_eofS =
        "\u01bd\uffff";
    static final String DFA14_minS =
        "\1\11\10\uffff\1\55\1\uffff\2\75\1\103\1\60\1\110\1\60\1\105\1\116"+
        "\1\120\1\111\1\60\1\116\1\125\1\60\1\101\2\60\1\122\1\101\1\122"+
        "\1\105\1\117\2\uffff\1\56\3\uffff\1\52\1\uffff\1\56\5\uffff\1\114"+
        "\1\110\1\117\1\60\1\117\1\114\1\117\1\122\1\124\1\111\1\105\1\60"+
        "\1\114\1\104\1\103\1\120\1\131\1\60\1\105\1\111\1\104\1\115\1\103"+
        "\1\105\1\114\1\125\1\104\1\60\1\124\1\117\1\60\1\102\1\125\1\130"+
        "\1\117\1\60\1\120\1\114\1\115\1\122\2\60\1\107\2\117\2\60\1\117"+
        "\1\125\1\111\1\114\1\101\1\126\1\137\1\uffff\1\56\3\uffff\1\105"+
        "\1\60\1\105\1\122\1\60\1\101\1\114\1\115\1\60\1\110\1\124\1\122"+
        "\2\60\1\105\3\60\1\114\1\60\1\uffff\1\105\1\60\1\105\1\116\1\60"+
        "\1\104\1\101\1\111\1\124\2\101\1\116\1\120\1\123\1\125\1\123\1\105"+
        "\1\uffff\1\60\1\111\1\122\1\60\1\114\1\116\1\124\1\60\1\uffff\2"+
        "\105\1\60\2\105\1\103\1\uffff\1\111\1\102\1\114\1\111\1\105\1\103"+
        "\1\60\1\120\1\102\1\115\1\103\1\125\1\116\1\117\1\101\1\56\1\103"+
        "\1\uffff\1\115\1\101\1\60\1\124\1\137\1\60\1\uffff\1\60\2\105\2"+
        "\uffff\1\122\3\uffff\1\111\1\131\1\uffff\1\120\1\130\1\uffff\1\60"+
        "\1\122\1\107\1\uffff\1\60\2\124\1\60\1\114\2\124\1\101\1\111\1\115"+
        "\1\124\1\122\1\117\1\125\1\137\1\105\1\103\1\60\1\116\1\60\1\uffff"+
        "\1\123\1\105\1\110\1\116\1\60\1\105\1\116\1\124\1\60\1\115\1\60"+
        "\1\114\1\101\1\110\1\116\1\105\1\124\1\113\1\103\1\56\1\124\1\101"+
        "\1\107\2\60\1\101\2\uffff\1\124\4\60\1\101\1\60\1\uffff\1\124\1"+
        "\60\1\uffff\1\105\1\60\1\uffff\1\137\1\105\1\60\1\103\1\123\1\116"+
        "\1\105\1\60\1\116\1\115\1\121\1\60\1\101\1\uffff\1\60\1\uffff\1"+
        "\125\1\124\2\60\1\124\1\uffff\1\101\1\60\1\105\1\uffff\1\111\1\101"+
        "\1\uffff\1\105\1\122\1\101\1\124\1\123\1\60\1\105\1\103\1\56\2\60"+
        "\1\105\1\60\1\uffff\1\103\1\111\4\uffff\1\103\1\uffff\1\60\1\uffff"+
        "\1\60\1\uffff\1\121\1\60\1\uffff\1\122\2\124\1\106\1\122\1\uffff"+
        "\2\60\1\125\1\uffff\1\124\1\uffff\1\111\1\101\1\uffff\1\60\1\116"+
        "\1\uffff\1\60\1\102\1\114\1\60\1\131\1\122\2\60\1\uffff\2\60\1\105"+
        "\1\56\2\uffff\2\60\1\103\1\115\1\105\2\uffff\1\125\1\uffff\2\60"+
        "\1\105\1\101\1\111\1\uffff\1\117\1\105\1\104\1\115\1\uffff\1\60"+
        "\1\uffff\1\105\1\60\1\uffff\2\60\4\uffff\1\123\1\56\1\uffff\1\55"+
        "\2\105\1\60\1\117\2\uffff\1\116\1\115\1\116\1\122\2\60\1\120\1\uffff"+
        "\1\60\3\uffff\1\123\1\55\1\123\1\60\1\122\1\103\1\111\1\107\1\125"+
        "\2\uffff\1\60\1\uffff\1\60\1\123\1\uffff\1\125\1\131\1\114\1\60"+
        "\1\115\2\uffff\1\60\1\115\1\60\1\131\1\uffff\1\60\1\uffff\1\60\1"+
        "\uffff\1\60";
    static final String DFA14_maxS =
        "\1\172\10\uffff\1\71\1\uffff\2\75\1\164\1\165\1\162\1\163\1\145"+
        "\1\156\1\165\1\157\2\162\1\165\1\146\2\171\2\162\1\141\1\162\1\145"+
        "\1\157\2\uffff\1\146\3\uffff\1\57\1\uffff\1\71\5\uffff\1\164\1\150"+
        "\1\157\1\146\1\157\1\154\1\157\1\162\1\164\1\151\1\145\1\146\1\164"+
        "\1\171\1\143\1\160\1\171\1\172\2\151\1\144\1\163\1\143\1\145\2\165"+
        "\1\144\1\172\1\164\1\157\1\146\1\142\1\165\1\170\1\157\1\172\1\160"+
        "\1\154\1\155\1\162\1\164\1\172\1\147\2\157\1\147\1\163\1\157\1\165"+
        "\1\151\1\162\1\141\1\166\1\137\1\uffff\1\146\3\uffff\1\145\1\172"+
        "\1\145\1\162\1\146\1\141\1\154\1\155\1\172\1\150\1\164\1\162\2\172"+
        "\1\145\3\172\1\154\1\172\1\uffff\1\145\1\172\1\145\1\156\1\172\1"+
        "\144\1\141\1\151\1\164\2\141\1\156\1\160\1\163\1\165\1\163\1\145"+
        "\1\uffff\1\172\1\151\1\162\1\150\1\154\1\156\1\164\1\172\1\uffff"+
        "\2\145\1\172\2\145\1\143\1\uffff\1\151\1\142\1\154\1\151\1\145\1"+
        "\143\1\151\1\160\1\142\1\155\1\151\1\165\1\156\1\157\1\141\1\146"+
        "\1\143\1\uffff\1\155\1\141\1\146\1\164\1\137\1\172\1\uffff\1\172"+
        "\2\145\2\uffff\1\162\3\uffff\1\151\1\171\1\uffff\1\160\1\170\1\uffff"+
        "\1\172\1\162\1\147\1\uffff\1\172\2\164\1\172\1\154\2\164\1\141\1"+
        "\151\1\155\1\164\1\162\1\157\1\165\1\137\1\145\1\143\1\172\1\156"+
        "\1\172\1\uffff\1\165\1\145\1\150\1\156\1\172\1\145\1\156\1\164\1"+
        "\172\1\155\1\172\1\154\1\141\1\150\1\156\1\145\1\164\1\153\1\143"+
        "\1\146\1\164\1\141\1\147\1\146\1\172\1\141\2\uffff\1\164\4\172\1"+
        "\141\1\172\1\uffff\1\164\1\172\1\uffff\1\145\1\172\1\uffff\1\137"+
        "\1\145\1\172\1\143\1\163\1\156\1\145\1\172\1\156\1\155\1\161\1\172"+
        "\1\141\1\uffff\1\172\1\uffff\1\165\1\164\2\172\1\164\1\uffff\1\141"+
        "\1\172\1\145\1\uffff\1\151\1\141\1\uffff\1\145\1\162\1\141\1\164"+
        "\1\163\1\172\1\145\1\143\1\146\2\172\1\145\1\146\1\uffff\1\143\1"+
        "\151\4\uffff\1\143\1\uffff\1\172\1\uffff\1\172\1\uffff\1\161\1\172"+
        "\1\uffff\1\162\2\164\1\146\1\162\1\uffff\2\172\1\165\1\uffff\1\164"+
        "\1\uffff\1\151\1\141\1\uffff\1\172\1\156\1\uffff\1\172\1\142\1\154"+
        "\1\172\1\171\1\162\2\172\1\uffff\2\172\1\145\1\146\2\uffff\1\172"+
        "\1\146\1\143\1\155\1\145\2\uffff\1\165\1\uffff\2\172\1\145\1\141"+
        "\1\151\1\uffff\1\157\1\145\1\144\1\155\1\uffff\1\172\1\uffff\1\145"+
        "\1\172\1\uffff\2\172\4\uffff\1\163\1\146\1\uffff\1\55\2\145\1\172"+
        "\1\157\2\uffff\1\156\1\155\1\156\1\162\2\172\1\160\1\uffff\1\172"+
        "\3\uffff\1\163\1\71\1\163\1\172\1\162\1\143\1\151\1\147\1\165\2"+
        "\uffff\1\172\1\uffff\1\172\1\163\1\uffff\1\165\1\171\1\154\1\172"+
        "\1\155\2\uffff\1\172\1\155\1\172\1\171\1\uffff\1\172\1\uffff\1\172"+
        "\1\uffff\1\172";
    static final String DFA14_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\uffff\1\12\26\uffff"+
        "\1\124\1\125\1\uffff\1\127\1\131\1\133\1\uffff\1\134\1\uffff\1\11"+
        "\1\14\1\13\1\15\1\16\66\uffff\1\126\1\uffff\1\132\1\130\1\135\24"+
        "\uffff\1\44\21\uffff\1\51\10\uffff\1\52\6\uffff\1\67\21\uffff\1"+
        "\36\6\uffff\1\77\3\uffff\1\62\1\31\1\uffff\1\22\1\33\1\70\2\uffff"+
        "\1\23\2\uffff\1\114\3\uffff\1\34\24\uffff\1\60\32\uffff\1\20\1\26"+
        "\7\uffff\1\55\2\uffff\1\116\2\uffff\1\100\15\uffff\1\115\1\uffff"+
        "\1\63\5\uffff\1\106\3\uffff\1\71\2\uffff\1\53\15\uffff\1\113\2\uffff"+
        "\1\21\1\61\1\104\1\40\1\uffff\1\50\1\uffff\1\30\1\uffff\1\27\2\uffff"+
        "\1\35\5\uffff\1\66\3\uffff\1\47\1\uffff\1\122\2\uffff\1\41\2\uffff"+
        "\1\37\10\uffff\1\72\4\uffff\1\17\1\46\5\uffff\1\24\1\25\1\uffff"+
        "\1\45\5\uffff\1\75\4\uffff\1\105\1\uffff\1\43\2\uffff\1\112\2\uffff"+
        "\1\120\1\56\1\73\1\74\2\uffff\1\65\5\uffff\1\110\1\64\7\uffff\1"+
        "\107\1\uffff\1\111\1\54\1\117\11\uffff\1\42\1\121\1\uffff\1\76\2"+
        "\uffff\1\123\5\uffff\1\57\1\102\4\uffff\1\103\1\uffff\1\101\1\uffff"+
        "\1\32\1\uffff";
    static final String DFA14_specialS =
        "\u01bd\uffff}>";
    static final String[] DFA14_transitionS = {
            "\2\46\2\uffff\1\46\22\uffff\1\46\1\uffff\1\42\4\uffff\1\41\1"+
            "\2\1\3\1\5\1\10\1\4\1\11\1\7\1\47\12\43\1\12\1\1\1\13\1\6\1"+
            "\14\1\44\1\uffff\1\20\1\32\1\25\1\33\1\30\1\16\1\36\1\45\1\22"+
            "\1\45\1\21\1\24\1\45\1\40\1\26\1\34\1\27\1\37\1\15\1\31\1\23"+
            "\1\35\1\17\3\45\6\uffff\1\20\1\32\1\25\1\33\1\30\1\16\1\36\1"+
            "\45\1\22\1\45\1\21\1\24\1\45\1\40\1\26\1\34\1\27\1\37\1\15\1"+
            "\31\1\23\1\35\1\17\3\45",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\50\2\uffff\12\51",
            "",
            "\1\53",
            "\1\55",
            "\1\60\1\uffff\1\57\16\uffff\1\61\16\uffff\1\60\1\uffff\1\57"+
            "\16\uffff\1\61",
            "\12\62\7\uffff\6\62\5\uffff\1\63\2\uffff\1\66\2\uffff\1\65"+
            "\2\uffff\1\64\13\uffff\6\62\5\uffff\1\63\2\uffff\1\66\2\uffff"+
            "\1\65\2\uffff\1\64",
            "\1\71\1\67\10\uffff\1\70\25\uffff\1\71\1\67\10\uffff\1\70",
            "\12\62\7\uffff\3\62\1\72\2\62\5\uffff\1\73\1\uffff\1\74\1\uffff"+
            "\1\76\2\uffff\1\75\15\uffff\3\62\1\72\2\62\5\uffff\1\73\1\uffff"+
            "\1\74\1\uffff\1\76\2\uffff\1\75",
            "\1\77\37\uffff\1\77",
            "\1\100\37\uffff\1\100",
            "\1\103\2\uffff\1\101\1\uffff\1\102\32\uffff\1\103\2\uffff\1"+
            "\101\1\uffff\1\102",
            "\1\104\5\uffff\1\105\31\uffff\1\104\5\uffff\1\105",
            "\12\62\7\uffff\6\62\5\uffff\1\110\2\uffff\1\107\2\uffff\1\106"+
            "\16\uffff\6\62\5\uffff\1\110\2\uffff\1\107\2\uffff\1\106",
            "\1\112\1\uffff\1\113\1\uffff\1\111\33\uffff\1\112\1\uffff\1"+
            "\113\1\uffff\1\111",
            "\1\114\37\uffff\1\114",
            "\12\62\7\uffff\1\115\5\62\32\uffff\1\115\5\62",
            "\1\116\3\uffff\1\120\2\uffff\1\126\1\125\5\uffff\1\122\2\uffff"+
            "\1\117\1\uffff\1\124\2\uffff\1\121\1\uffff\1\123\7\uffff\1\116"+
            "\3\uffff\1\120\2\uffff\1\126\1\125\5\uffff\1\122\2\uffff\1\117"+
            "\1\uffff\1\124\2\uffff\1\121\1\uffff\1\123",
            "\12\62\7\uffff\1\127\3\62\1\134\1\62\2\uffff\1\131\2\uffff"+
            "\1\132\2\uffff\1\133\11\uffff\1\130\7\uffff\1\127\3\62\1\134"+
            "\1\62\2\uffff\1\131\2\uffff\1\132\2\uffff\1\133\11\uffff\1\130",
            "\12\62\7\uffff\4\62\1\135\1\62\10\uffff\1\137\2\uffff\1\136"+
            "\16\uffff\4\62\1\135\1\62\10\uffff\1\137\2\uffff\1\136",
            "\1\140\37\uffff\1\140",
            "\1\141\37\uffff\1\141",
            "\1\142\37\uffff\1\142",
            "\1\143\37\uffff\1\143",
            "\1\144\37\uffff\1\144",
            "",
            "",
            "\1\150\1\uffff\12\146\7\uffff\6\147\32\uffff\6\147",
            "",
            "",
            "",
            "\1\151\4\uffff\1\50",
            "",
            "\1\150\1\uffff\12\51",
            "",
            "",
            "",
            "",
            "",
            "\1\152\7\uffff\1\153\27\uffff\1\152\7\uffff\1\153",
            "\1\154\37\uffff\1\154",
            "\1\155\37\uffff\1\155",
            "\12\156\7\uffff\6\156\32\uffff\6\156",
            "\1\157\37\uffff\1\157",
            "\1\160\37\uffff\1\160",
            "\1\161\37\uffff\1\161",
            "\1\162\37\uffff\1\162",
            "\1\163\37\uffff\1\163",
            "\1\164\37\uffff\1\164",
            "\1\165\37\uffff\1\165",
            "\12\156\7\uffff\3\156\1\166\2\156\32\uffff\3\156\1\166\2\156",
            "\1\167\7\uffff\1\170\27\uffff\1\167\7\uffff\1\170",
            "\1\171\24\uffff\1\172\12\uffff\1\171\24\uffff\1\172",
            "\1\173\37\uffff\1\173",
            "\1\174\37\uffff\1\174",
            "\1\175\37\uffff\1\175",
            "\12\45\7\uffff\3\45\1\177\16\45\1\u0081\1\u0080\6\45\4\uffff"+
            "\1\45\1\uffff\3\45\1\177\16\45\1\u0081\1\u0080\6\45",
            "\1\u0083\3\uffff\1\u0082\33\uffff\1\u0083\3\uffff\1\u0082",
            "\1\u0084\37\uffff\1\u0084",
            "\1\u0085\37\uffff\1\u0085",
            "\1\u0086\5\uffff\1\u0087\31\uffff\1\u0086\5\uffff\1\u0087",
            "\1\u0088\37\uffff\1\u0088",
            "\1\u0089\37\uffff\1\u0089",
            "\1\u008d\1\u008b\1\u008c\6\uffff\1\u008a\26\uffff\1\u008d\1"+
            "\u008b\1\u008c\6\uffff\1\u008a",
            "\1\u008e\37\uffff\1\u008e",
            "\1\u008f\37\uffff\1\u008f",
            "\12\45\7\uffff\4\45\1\u0091\25\45\4\uffff\1\45\1\uffff\4\45"+
            "\1\u0091\25\45",
            "\1\u0092\37\uffff\1\u0092",
            "\1\u0093\37\uffff\1\u0093",
            "\12\156\7\uffff\2\156\1\u0094\3\156\32\uffff\2\156\1\u0094"+
            "\3\156",
            "\1\u0095\37\uffff\1\u0095",
            "\1\u0096\37\uffff\1\u0096",
            "\1\u0097\37\uffff\1\u0097",
            "\1\u0098\37\uffff\1\u0098",
            "\12\45\7\uffff\12\45\1\u009a\17\45\4\uffff\1\45\1\uffff\12"+
            "\45\1\u009a\17\45",
            "\1\u009b\37\uffff\1\u009b",
            "\1\u009c\37\uffff\1\u009c",
            "\1\u009d\37\uffff\1\u009d",
            "\1\u009e\37\uffff\1\u009e",
            "\12\156\7\uffff\6\156\15\uffff\1\u009f\14\uffff\6\156\15\uffff"+
            "\1\u009f",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00a1\37\uffff\1\u00a1",
            "\1\u00a2\37\uffff\1\u00a2",
            "\1\u00a3\37\uffff\1\u00a3",
            "\12\156\7\uffff\6\156\1\u00a4\31\uffff\6\156\1\u00a4",
            "\12\156\7\uffff\2\156\1\u00a7\3\156\5\uffff\1\u00a5\6\uffff"+
            "\1\u00a6\15\uffff\2\156\1\u00a7\3\156\5\uffff\1\u00a5\6\uffff"+
            "\1\u00a6",
            "\1\u00a8\37\uffff\1\u00a8",
            "\1\u00a9\37\uffff\1\u00a9",
            "\1\u00aa\37\uffff\1\u00aa",
            "\1\u00ac\5\uffff\1\u00ab\31\uffff\1\u00ac\5\uffff\1\u00ab",
            "\1\u00ad\37\uffff\1\u00ad",
            "\1\u00ae\37\uffff\1\u00ae",
            "\1\u00af",
            "",
            "\1\150\1\uffff\12\u00b0\7\uffff\6\147\32\uffff\6\147",
            "",
            "",
            "",
            "\1\u00b1\37\uffff\1\u00b1",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00b3\37\uffff\1\u00b3",
            "\1\u00b4\37\uffff\1\u00b4",
            "\12\u00b5\7\uffff\6\u00b5\32\uffff\6\u00b5",
            "\1\u00b6\37\uffff\1\u00b6",
            "\1\u00b7\37\uffff\1\u00b7",
            "\1\u00b8\37\uffff\1\u00b8",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00ba\37\uffff\1\u00ba",
            "\1\u00bb\37\uffff\1\u00bb",
            "\1\u00bc\37\uffff\1\u00bc",
            "\12\u00b5\7\uffff\6\u00b5\24\45\4\uffff\1\45\1\uffff\6\u00b5"+
            "\24\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00bf\37\uffff\1\u00bf",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\10\45\1\u00c3\21\45\4\uffff\1\45\1\uffff\10"+
            "\45\1\u00c3\21\45",
            "\1\u00c4\37\uffff\1\u00c4",
            "\12\45\7\uffff\22\45\1\u00c6\7\45\4\uffff\1\45\1\uffff\22\45"+
            "\1\u00c6\7\45",
            "",
            "\1\u00c7\37\uffff\1\u00c7",
            "\12\45\7\uffff\16\45\1\u00c9\13\45\4\uffff\1\45\1\uffff\16"+
            "\45\1\u00c9\13\45",
            "\1\u00ca\37\uffff\1\u00ca",
            "\1\u00cb\37\uffff\1\u00cb",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00cd\37\uffff\1\u00cd",
            "\1\u00ce\37\uffff\1\u00ce",
            "\1\u00cf\37\uffff\1\u00cf",
            "\1\u00d0\37\uffff\1\u00d0",
            "\1\u00d1\37\uffff\1\u00d1",
            "\1\u00d2\37\uffff\1\u00d2",
            "\1\u00d3\37\uffff\1\u00d3",
            "\1\u00d4\37\uffff\1\u00d4",
            "\1\u00d5\37\uffff\1\u00d5",
            "\1\u00d6\37\uffff\1\u00d6",
            "\1\u00d7\37\uffff\1\u00d7",
            "\1\u00d8\37\uffff\1\u00d8",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00d9\37\uffff\1\u00d9",
            "\1\u00da\37\uffff\1\u00da",
            "\12\u00b5\7\uffff\6\u00b5\1\uffff\1\u00db\30\uffff\6\u00b5"+
            "\1\uffff\1\u00db",
            "\1\u00dc\37\uffff\1\u00dc",
            "\1\u00dd\37\uffff\1\u00dd",
            "\1\u00de\37\uffff\1\u00de",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u00df\37\uffff\1\u00df",
            "\1\u00e0\37\uffff\1\u00e0",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00e2\37\uffff\1\u00e2",
            "\1\u00e3\37\uffff\1\u00e3",
            "\1\u00e4\37\uffff\1\u00e4",
            "",
            "\1\u00e5\37\uffff\1\u00e5",
            "\1\u00e6\37\uffff\1\u00e6",
            "\1\u00e7\37\uffff\1\u00e7",
            "\1\u00e8\37\uffff\1\u00e8",
            "\1\u00e9\37\uffff\1\u00e9",
            "\1\u00ea\37\uffff\1\u00ea",
            "\12\u00b5\7\uffff\6\u00b5\2\uffff\1\u00eb\27\uffff\6\u00b5"+
            "\2\uffff\1\u00eb",
            "\1\u00ec\37\uffff\1\u00ec",
            "\1\u00ed\37\uffff\1\u00ed",
            "\1\u00ee\37\uffff\1\u00ee",
            "\1\u00ef\5\uffff\1\u00f0\31\uffff\1\u00ef\5\uffff\1\u00f0",
            "\1\u00f1\37\uffff\1\u00f1",
            "\1\u00f2\37\uffff\1\u00f2",
            "\1\u00f3\37\uffff\1\u00f3",
            "\1\u00f4\37\uffff\1\u00f4",
            "\1\150\1\uffff\12\u00f5\7\uffff\6\147\32\uffff\6\147",
            "\1\u00f6\37\uffff\1\u00f6",
            "",
            "\1\u00f7\37\uffff\1\u00f7",
            "\1\u00f8\37\uffff\1\u00f8",
            "\12\u00f9\7\uffff\6\u00f9\32\uffff\6\u00f9",
            "\1\u00fa\37\uffff\1\u00fa",
            "\1\u00fb",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u00fe\37\uffff\1\u00fe",
            "\1\u00ff\37\uffff\1\u00ff",
            "",
            "",
            "\1\u0100\37\uffff\1\u0100",
            "",
            "",
            "",
            "\1\u0101\37\uffff\1\u0101",
            "\1\u0102\37\uffff\1\u0102",
            "",
            "\1\u0103\37\uffff\1\u0103",
            "\1\u0104\37\uffff\1\u0104",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0106\37\uffff\1\u0106",
            "\1\u0107\37\uffff\1\u0107",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0109\37\uffff\1\u0109",
            "\1\u010a\37\uffff\1\u010a",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u010c\37\uffff\1\u010c",
            "\1\u010d\37\uffff\1\u010d",
            "\1\u010e\37\uffff\1\u010e",
            "\1\u010f\37\uffff\1\u010f",
            "\1\u0110\37\uffff\1\u0110",
            "\1\u0111\37\uffff\1\u0111",
            "\1\u0112\37\uffff\1\u0112",
            "\1\u0113\37\uffff\1\u0113",
            "\1\u0114\37\uffff\1\u0114",
            "\1\u0115\37\uffff\1\u0115",
            "\1\u0116",
            "\1\u0117\37\uffff\1\u0117",
            "\1\u0118\37\uffff\1\u0118",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u011a\37\uffff\1\u011a",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u011d\1\uffff\1\u011c\35\uffff\1\u011d\1\uffff\1\u011c",
            "\1\u011e\37\uffff\1\u011e",
            "\1\u011f\37\uffff\1\u011f",
            "\1\u0120\37\uffff\1\u0120",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0122\37\uffff\1\u0122",
            "\1\u0123\37\uffff\1\u0123",
            "\1\u0124\37\uffff\1\u0124",
            "\12\45\7\uffff\21\45\1\u0126\10\45\4\uffff\1\45\1\uffff\21"+
            "\45\1\u0126\10\45",
            "\1\u0127\37\uffff\1\u0127",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0129\37\uffff\1\u0129",
            "\1\u012a\37\uffff\1\u012a",
            "\1\u012b\37\uffff\1\u012b",
            "\1\u012c\37\uffff\1\u012c",
            "\1\u012d\37\uffff\1\u012d",
            "\1\u012e\37\uffff\1\u012e",
            "\1\u012f\37\uffff\1\u012f",
            "\1\u0130\37\uffff\1\u0130",
            "\1\150\1\uffff\12\u0131\7\uffff\6\147\32\uffff\6\147",
            "\1\u0132\37\uffff\1\u0132",
            "\1\u0133\37\uffff\1\u0133",
            "\1\u0134\37\uffff\1\u0134",
            "\12\u0135\7\uffff\6\u0135\32\uffff\6\u0135",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0137\37\uffff\1\u0137",
            "",
            "",
            "\1\u0138\37\uffff\1\u0138",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u013d\37\uffff\1\u013d",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u013f\37\uffff\1\u013f",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u0141\37\uffff\1\u0141",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u0143",
            "\1\u0144\37\uffff\1\u0144",
            "\12\45\7\uffff\4\45\1\u0146\25\45\4\uffff\1\45\1\uffff\4\45"+
            "\1\u0146\25\45",
            "\1\u0147\37\uffff\1\u0147",
            "\1\u0148\37\uffff\1\u0148",
            "\1\u0149\37\uffff\1\u0149",
            "\1\u014a\37\uffff\1\u014a",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u014c\37\uffff\1\u014c",
            "\1\u014d\37\uffff\1\u014d",
            "\1\u014e\37\uffff\1\u014e",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0150\37\uffff\1\u0150",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u0152\37\uffff\1\u0152",
            "\1\u0153\37\uffff\1\u0153",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0155\37\uffff\1\u0155",
            "",
            "\1\u0156\37\uffff\1\u0156",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0158\37\uffff\1\u0158",
            "",
            "\1\u0159\37\uffff\1\u0159",
            "\1\u015a\37\uffff\1\u015a",
            "",
            "\1\u015b\37\uffff\1\u015b",
            "\1\u015c\37\uffff\1\u015c",
            "\1\u015d\37\uffff\1\u015d",
            "\1\u015e\37\uffff\1\u015e",
            "\1\u015f\37\uffff\1\u015f",
            "\12\45\7\uffff\22\45\1\u0161\7\45\4\uffff\1\45\1\uffff\22\45"+
            "\1\u0161\7\45",
            "\1\u0162\37\uffff\1\u0162",
            "\1\u0163\37\uffff\1\u0163",
            "\1\150\1\uffff\12\u0164\7\uffff\6\147\32\uffff\6\147",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0167\37\uffff\1\u0167",
            "\12\u0168\7\uffff\6\u0168\32\uffff\6\u0168",
            "",
            "\1\u0169\37\uffff\1\u0169",
            "\1\u016a\37\uffff\1\u016a",
            "",
            "",
            "",
            "",
            "\1\u016b\37\uffff\1\u016b",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u016e\37\uffff\1\u016e",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u0170\37\uffff\1\u0170",
            "\1\u0171\37\uffff\1\u0171",
            "\1\u0172\37\uffff\1\u0172",
            "\1\u0173\37\uffff\1\u0173",
            "\1\u0174\37\uffff\1\u0174",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0176\37\uffff\1\u0176",
            "",
            "\1\u0177\37\uffff\1\u0177",
            "",
            "\1\u0178\37\uffff\1\u0178",
            "\1\u0179\37\uffff\1\u0179",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u017b\37\uffff\1\u017b",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u017d\37\uffff\1\u017d",
            "\1\u017e\37\uffff\1\u017e",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0180\37\uffff\1\u0180",
            "\1\u0181\37\uffff\1\u0181",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0186\37\uffff\1\u0186",
            "\1\150\1\uffff\12\u0187\7\uffff\6\147\32\uffff\6\147",
            "",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\u0189\7\uffff\6\u0189\32\uffff\6\u0189",
            "\1\u018a\37\uffff\1\u018a",
            "\1\u018b\37\uffff\1\u018b",
            "\1\u018c\37\uffff\1\u018c",
            "",
            "",
            "\1\u018d\37\uffff\1\u018d",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u0190\37\uffff\1\u0190",
            "\1\u0191\37\uffff\1\u0191",
            "\1\u0192\37\uffff\1\u0192",
            "",
            "\1\u0193\37\uffff\1\u0193",
            "\1\u0194\37\uffff\1\u0194",
            "\1\u0195\37\uffff\1\u0195",
            "\1\u0196\37\uffff\1\u0196",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\1\u0198\37\uffff\1\u0198",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "",
            "",
            "",
            "\1\u019c\37\uffff\1\u019c",
            "\1\150\1\uffff\12\u019d\7\uffff\6\147\32\uffff\6\147",
            "",
            "\1\147",
            "\1\u019e\37\uffff\1\u019e",
            "\1\u019f\37\uffff\1\u019f",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01a0\37\uffff\1\u01a0",
            "",
            "",
            "\1\u01a1\37\uffff\1\u01a1",
            "\1\u01a2\37\uffff\1\u01a2",
            "\1\u01a3\37\uffff\1\u01a3",
            "\1\u01a4\37\uffff\1\u01a4",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01a7\37\uffff\1\u01a7",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "",
            "",
            "\1\u01a9\37\uffff\1\u01a9",
            "\1\147\1\150\1\uffff\12\51",
            "\1\u01aa\37\uffff\1\u01aa",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01ac\37\uffff\1\u01ac",
            "\1\u01ad\37\uffff\1\u01ad",
            "\1\u01ae\37\uffff\1\u01ae",
            "\1\u01af\37\uffff\1\u01af",
            "\1\u01b0\37\uffff\1\u01b0",
            "",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01b3\37\uffff\1\u01b3",
            "",
            "\1\u01b4\37\uffff\1\u01b4",
            "\1\u01b5\37\uffff\1\u01b5",
            "\1\u01b6\37\uffff\1\u01b6",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01b8\37\uffff\1\u01b8",
            "",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01ba\37\uffff\1\u01ba",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\u01bc\37\uffff\1\u01bc",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45"
    };

    static final short[] DFA14_eot = DFA.unpackEncodedString(DFA14_eotS);
    static final short[] DFA14_eof = DFA.unpackEncodedString(DFA14_eofS);
    static final char[] DFA14_min = DFA.unpackEncodedStringToUnsignedChars(DFA14_minS);
    static final char[] DFA14_max = DFA.unpackEncodedStringToUnsignedChars(DFA14_maxS);
    static final short[] DFA14_accept = DFA.unpackEncodedString(DFA14_acceptS);
    static final short[] DFA14_special = DFA.unpackEncodedString(DFA14_specialS);
    static final short[][] DFA14_transition;

    static {
        int numStates = DFA14_transitionS.length;
        DFA14_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA14_transition[i] = DFA.unpackEncodedString(DFA14_transitionS[i]);
        }
    }

    class DFA14 extends DFA {

        public DFA14(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 14;
            this.eot = DFA14_eot;
            this.eof = DFA14_eof;
            this.min = DFA14_min;
            this.max = DFA14_max;
            this.accept = DFA14_accept;
            this.special = DFA14_special;
            this.transition = DFA14_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__112 | T__113 | T__114 | T__115 | T__116 | T__117 | T__118 | T__119 | T__120 | T__121 | T__122 | T__123 | T__124 | T__125 | K_SELECT | K_FROM | K_WHERE | K_AND | K_KEY | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_ALL | K_CONSISTENCY | K_LEVEL | K_USE | K_COUNT | K_SET | K_BEGIN | K_APPLY | K_BATCH | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_COLUMNFAMILY | K_INDEX | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_GRANT | K_GRANTS | K_REVOKE | K_OPTION | K_DESCRIBE | K_FOR | K_LIST | K_FULL_ACCESS | K_NO_ACCESS | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | IDENT | UUID | WS | COMMENT | MULTILINE_COMMENT );";
        }
    }
 

}