create or replace procedure cda_merged_views_sp(raw_schema varchar(255),merged_schema varchar(255))
as $$

DECLARE rec1 record;
		rec2 record;
		_query character varying;
		
-- create work table 1 to group raw schema table objects into 4 groups (01-TypeList, 02-Non-TypeList Retirable, 03-EffDated groups)	

BEGIN

-- create work table to get the list of current state views in the merged schema

    DROP TABLE IF EXISTS tmp_table1;
	
    CREATE TABLE tmp_table1 AS
    SELECT * FROM information_schema.tables 
	WHERE  table_schema = merged_schema 
	and table_type = 'VIEW' 
	and left(table_name,8) = 'vw_curr_'; 

-- create work table 1 to group raw schema table objects into 4 groups (01-TypeList, 02-Non-TypeList Retirable, 03-EffDated groups)	

	DROP TABLE IF EXISTS tmp_table2;
	
	CREATE TABLE tmp_table2 AS
	SELECT DISTINCT '01-retirable_ctlentity' as entity_grp,
	t.table_name as table_name
	FROM information_schema.columns c
	JOIN information_schema.tables t 
	on c.table_schema = t.table_schema 
	and c.table_name = t.table_name
	WHERE  t.table_schema = raw_schema 
	and c.column_name = 'retired' 
	and left(t.table_name,5) in ('pctl_','cctl_','bctl_','abtl_') 
	and left(t.table_name,5) not in ('pcst_','ccst_','bcst_','abst_') 
	and t.table_type = 'BASE TABLE'
	UNION
	SELECT DISTINCT '02-retirable_nonctlentity' as entity_grp, 
	t.table_name as table_name
	FROM information_schema.columns c
	JOIN information_schema.tables t 
	on c.table_schema = t.table_schema 
	and c.table_name = t.table_name
	WHERE t.table_schema = raw_schema 
	and c.column_name = 'retired' 
	and left(t.table_name,5) 
	not in ('pctl_','cctl_','bctl_','abtl_','pcst_','vw_cu','pcst_','ccst_','bcst_','abst_') 
	and t.table_type = 'BASE TABLE'
	UNION
	SELECT  DISTINCT '03-effdated_entity' as entity_grp, 
	t.table_name as table_name
	FROM information_schema.columns c
	JOIN information_schema.tables t 
	on c.table_schema = t.table_schema 
	and c.table_name = t.table_name
	WHERE t.table_schema = raw_schema 
	and c.column_name = 'branchid' 
	and left(t.table_name,5) not in ('pctl_','pcst_','vw_cu','pcst_') 
	and t.table_type = 'BASE TABLE' 
	and t.table_name <> 'pc_producercode';

-- create work table to identify the remaining raw schema table objects that do not belong to the first 3 groups and lable the group 04-allother_entities

	DROP TABLE IF EXISTS tmp_table3;
	
	CREATE TABLE tmp_table3 AS
	SElECT DISTINCT '04-allother_entities' as entity_grp, 
	l.table_name as table_name
	FROM (SELECT DISTINCT table_name FROM information_schema.tables 
		  WHERE table_schema = raw_schema 
		  and table_type = 'BASE TABLE' 
		  and left(table_name,5) not in ('pcst_','ccst_','bcst_','abst_') 
		  ) l
	LEFT JOIN tmp_table2 r 
	on l.table_name = r.table_name
	WHERE (left(l.table_name,8) <> 'vw_curr_' 
	and r.table_name is null) 
	or (left(l.table_name,5) 
	not in ('pcst_', 'ccst_','bcst_','abst_') 
	and r.table_name is null);

--Union the 2 work tables to create a single work table for all raw schema objects and assign a current state view name for each object
	DROP TABLE IF EXISTS tmp_table4;
	
	CREATE TABLE tmp_table4 AS
	SELECT x.entity_grp as entity_grp, 
	x.table_name as table_name,
	x.curr_view_name as curr_view_name,
	raw_schema as raw_schema_nm, 
	merged_schema as merged_schema_nm
	FROM (	SELECT t2.entity_grp as entity_grp,  
			t2.table_name as table_name, 
		    'vw_curr_' || t2.table_name as curr_view_name
			FROM tmp_table2 t2 UNION
			SELECT t3.entity_grp as entity_grp, 
			t3.table_name as table_name,
		    'vw_curr_' || t3.table_name as curr_view_name
			FROM tmp_table3 t3) x
	ORDER BY x.entity_grp,x.table_name;
	
	-- Loop thru the list of existing curr state views in the merged schema and drop  them
	FOR rec1 IN SELECT table_schema, table_name FROM tmp_table1
    LOOP
        EXECUTE 'drop view ' || quote_ident(rec1.table_schema) || '.' || quote_ident(rec1.table_name);
    END LOOP;

-- Loop thru temp table 3 and create current row views for every raw schema object.
-- In this version the simplest from of the veiw is being generated for all the 4 group types. 
-- The Current State is initialized to max(gwcbi___seqval_hex) for each id (pk value) in each InsuranceSuite Application raw schema entity
-- Additional variants to assign the current row to one record per id, updatetime combination to preserve more informational contents for unbound effective dated entities, claim non-financial 
-- entities, and Billing non-transaction and context entities. see the accompanying readme file for details
 
	FOR rec2 IN SELECT entity_grp, raw_schema as raw_schema_nm, merged_schema as merged_schema_nm, table_name,curr_view_name FROM tmp_table4  
	LOOP
	_query := 'create or replace view ' || rec2.merged_schema_nm || '.' || rec2.curr_view_name || ' AS
				SELECT t0.* 
 					FROM (SELECT row_number() OVER (PARTITION BY id order by lpad(gwcbi___seqval_hex, 32,' || '''0''' || ') desc) as sequencing_num, 
								* 
			 				FROM ' || rec2.raw_schema_nm || '.' || rec2.table_name || ') t0 
 				where t0.sequencing_num = 1' ;
	EXECUTE _query;
	END LOOP;

END;
$$ language plpgsql;