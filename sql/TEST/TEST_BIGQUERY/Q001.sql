MERGE INTO @PROJECT@.TEST.T1 tgt
USING (SELECT '@DAG_NAME@' AS dag_name, CURRENT_TIMESTAMP AS last_run) AS src
ON tgt.dag_name = src.dag_name
WHEN MATCHED THEN
	UPDATE
	SET tgt.last_run = src.last_run
WHEN NOT MATCHED BY TARGET THEN
INSERT
(dag_name, last_run)
VALUES
(src.dag_name, src.last_run);