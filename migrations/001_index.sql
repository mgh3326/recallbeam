CREATE TABLE rb_state (source VARCHAR2(64) PRIMARY KEY, active_slot NUMBER(1) DEFAULT 0 NOT NULL CHECK (active_slot IN (0,1)))
/
INSERT INTO rb_state(source,active_slot) VALUES ('hk',0)
/
CREATE TABLE rb_writer (id NUMBER PRIMARY KEY)
/
INSERT INTO rb_writer(id) VALUES (1)
/
CREATE TABLE rb_docs_a (
 source VARCHAR2(64) NOT NULL, model VARCHAR2(32) NOT NULL,
 model_id VARCHAR2(256) NOT NULL, doc_key VARCHAR2(1024), identifier VARCHAR2(256), title VARCHAR2(2048),
 body CLOB, tags VARCHAR2(2048), updated_at TIMESTAMP WITH TIME ZONE,
 source_sync_id NUMBER(19) NOT NULL, search_text CLOB,
 PRIMARY KEY(source,model,model_id)
)
/
CREATE TABLE rb_docs_b (
 source VARCHAR2(64) NOT NULL, model VARCHAR2(32) NOT NULL,
 model_id VARCHAR2(256) NOT NULL, doc_key VARCHAR2(1024), identifier VARCHAR2(256), title VARCHAR2(2048),
 body CLOB, tags VARCHAR2(2048), updated_at TIMESTAMP WITH TIME ZONE,
 source_sync_id NUMBER(19) NOT NULL, search_text CLOB,
 PRIMARY KEY(source,model,model_id)
)
/
CREATE TABLE rb_cursor_a (source VARCHAR2(64) PRIMARY KEY, last_sync_id NUMBER(19) NOT NULL, schema_version NUMBER NOT NULL)
/
CREATE TABLE rb_cursor_b (source VARCHAR2(64) PRIMARY KEY, last_sync_id NUMBER(19) NOT NULL, schema_version NUMBER NOT NULL)
/
BEGIN
 CTX_DDL.CREATE_PREFERENCE('rb_lexer','AUTO_LEXER');
END;
/
CREATE INDEX rb_text_a ON rb_docs_a(search_text) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('LEXER rb_lexer STOPLIST CTXSYS.EMPTY_STOPLIST SYNC (ON COMMIT)')
/
CREATE INDEX rb_text_b ON rb_docs_b(search_text) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('LEXER rb_lexer STOPLIST CTXSYS.EMPTY_STOPLIST SYNC (ON COMMIT)')
/
CREATE VIEW rb_docs AS
 SELECT x.source,x.model,x.model_id,NVL(a.doc_key,b.doc_key) doc_key,
 NVL(a.identifier,b.identifier) identifier,NVL(a.title,b.title) title,
 NVL(a.body,b.body) body,NVL(a.tags,b.tags) tags,
 NVL(a.updated_at,b.updated_at) updated_at,NVL(a.source_sync_id,b.source_sync_id) source_sync_id
 FROM (
 SELECT d.source,d.model,d.model_id,0 slot FROM rb_docs_a d JOIN rb_state s ON s.source=d.source AND s.active_slot=0
 UNION ALL
 SELECT d.source,d.model,d.model_id,1 slot FROM rb_docs_b d JOIN rb_state s ON s.source=d.source AND s.active_slot=1
 ) x
 LEFT JOIN rb_docs_a a ON x.slot=0 AND a.source=x.source AND a.model=x.model AND a.model_id=x.model_id
 LEFT JOIN rb_docs_b b ON x.slot=1 AND b.source=x.source AND b.model=x.model AND b.model_id=x.model_id
/
CREATE VIEW rb_cursor AS
 SELECT c.* FROM rb_cursor_a c JOIN rb_state s ON s.source=c.source AND s.active_slot=0
 UNION ALL
 SELECT c.* FROM rb_cursor_b c JOIN rb_state s ON s.source=c.source AND s.active_slot=1
/
