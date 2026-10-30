-- id-insensitive content digests (surrogate ids replaced by natural keys: raw_id for table1, text for table3/4)
SELECT 't3', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT concat_ws(',', text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4) x FROM table3) s
UNION ALL SELECT 't4', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT t::text x FROM table4 t) s
UNION ALL SELECT 't1', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT (to_jsonb(t) - 'id_column_1' - 'id_column_4' || jsonb_build_object('t3', d.text_column_1 || d.text_column_3 || d.text_column_4))::text x FROM table1 t LEFT JOIN table3 d ON d.id_column_1 = t.id_column_4) s
UNION ALL SELECT 't2', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT (to_jsonb(t) - 'id_column_1' - 'id_column_2' || jsonb_build_object('r', m.id_column_3))::text x FROM table2 t JOIN table1 m ON m.id_column_1 = t.id_column_2) s
UNION ALL SELECT 't5', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT (to_jsonb(t) - 'id_column_1' - 'id_column_2' || jsonb_build_object('r', m.id_column_3))::text x FROM table5 t JOIN table1 m ON m.id_column_1 = t.id_column_2) s
UNION ALL SELECT 't6', count(*), md5(string_agg(x, '|' ORDER BY x)) FROM (SELECT (to_jsonb(t) - 'id_column_1' - 'id_column_2' || jsonb_build_object('r', m.id_column_3))::text x FROM table6 t JOIN table1 m ON m.id_column_1 = t.id_column_2) s
ORDER BY 1;
