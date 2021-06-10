from cmd.sql_cmd import SqlCmd
from core.core import DatabaseType, Query


class DescCmd(SqlCmd):
    name = 'desc'

    def get_sqlserver_index_information_dict(self, conn, tab_name, dbo='dbo'):
        index_formation = self._exe_query(
            f"""SELECT c.name colName,object_name(constraint_object_id) constName,'FK' type,object_name(referenced_object_id) refTabName,c1.name refColName 
                FROM sys.foreign_key_columns f 
                JOIN sys.tables t ON t.object_id=f.parent_object_id 
                JOIN sys.columns c ON c.object_id=f.parent_object_id AND c.column_id=f.parent_column_id 
                JOIN sys.columns c1 ON c1.object_id=f.referenced_object_id AND c1.column_id=f.referenced_column_id 
                JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='{tab_name}' AND s.name='{dbo}' 
                UNION ALL 
                SELECT COLUMN_NAME colName,CONSTRAINT_NAME constName,k.type,NULL refTabName,NULL refColName 
                FROM sys.key_constraints k 
                LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c ON k.name=c.CONSTRAINT_NAME 
                WHERE c.TABLE_SCHEMA='{dbo}' AND c.TABLE_NAME='{tab_name}'""",
            conn)[1]
        # {colName:(colName,constName,indexType,refTabName,refColName)}
        return {fmt[0]: fmt for fmt in index_formation[0]} if index_formation else dict(), \
               {fmt[0]: fmt for fmt in index_formation[0] if fmt[2] == 'FK'} if index_formation else dict()

    def print_table_description(self, conn, qy: Query):
        sql = f"""SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT 
                  FROM information_schema.columns 
                  WHERE table_schema='{qy.database}' AND table_name='{qy.table_name}'"""
        if qy.server_type is DatabaseType.SQL_SERVER:
            sql = f"""SELECT col.name,t.name dataType,isc.CHARACTER_MAXIMUM_LENGTH,isc.NUMERIC_PRECISION,isc.NUMERIC_SCALE,CASE WHEN col.isnullable=1 THEN 'YES' ELSE 'NO' END nullable,comm.text defVal,CASE WHEN COLUMNPROPERTY(col.id,col.name,'IsIdentity')=1 THEN 'IDENTITY' ELSE '' END Extra,ISNULL(CONVERT(varchar,ep.value), '') comment 
                      FROM dbo.syscolumns col 
                      LEFT JOIN dbo.systypes t ON col.xtype=t.xusertype 
                      JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 
                      LEFT JOIN dbo.syscomments comm ON col.cdefault=comm.id 
                      LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id AND ep.name='MS_Description' 
                      LEFT JOIN information_schema.columns isc ON obj.name=isc.TABLE_NAME AND col.name=isc.COLUMN_NAME 
                      WHERE isc.TABLE_CATALOG='{qy.database}' AND obj.name='{qy.table_name}' ORDER BY col.colorder"""
        res = self._exe_query(sql, conn)[1]
        if not res or not res[0]:
            self.printer.print_error_msg(f"{qy.table_name} not found!")
            return
        res = [list(r) for r in res[0]]
        if qy.server_type is DatabaseType.SQL_SERVER:
            index_dict, foreign_dict = self.get_sqlserver_index_information_dict(conn, qy.table_name)
            for row in res:
                data_type, cml, np, ns = row[1], row.pop(2), row.pop(2), row.pop(2)
                if cml and row[1] not in {'text', 'ntext', 'xml'}:
                    row[1] = f'{row[1]}({"max" if cml == -1 else cml})'
                elif data_type in ('decimal', 'numeric'):
                    row[1] = f'{row[1]}({np},{ns})'
                t = index_dict.get(row[0], ('', '', '', '', ''))
                f = foreign_dict.get(row[0], None)
                if t[2] == 'PK':
                    key = 'PK'
                elif t[2] == 'UQ':
                    key, row[4] = 'UQ', t[1]
                else:
                    key = ''
                if f:
                    key = f'{key},FK' if key else 'FK'
                    row[4] = f'{row[4]},REFERENCES {f[3]}.{f[4]}' if row[4] else f'REFERENCES {f[3]}.{f[4]}'
                row.insert(3, key)
                row[4] = '' if row[4] is None else row[4]
        else:
            for row in res:
                row[4] = '' if row[4] is None else row[4]
        self.print_result_set(['Name', 'Type', 'Nullable', 'Key', 'Default', 'Extra', 'Comment'], res, qy)

    def get_create_table_ddl(self, conn, query: Query):
        def get_create_table_mysql_ddl():
            res = self._exe_no_query(f'show create table {query.table_name}', conn)[2]
            if not res:
                return None
            return res[0][0][1] if len(res[0][0]) == 2 else res[0][0][0]

        def get_create_table_sql_server_ddl():
            def is_number(_data_type):
                return _data_type in {'bit', 'int', 'tinyint', 'smallint', 'bigint', 'float',
                                      'decimal', 'numeric', 'real', 'money', 'smallmoney'}

            res_list = []
            sql = f"""sp_columns {query.table_name};
                      SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,ic.IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,tmp.definition 
                      FROM information_schema.columns ic 
                      LEFT JOIN (
                           SELECT c.name col,c.definition 
                           FROM sys.computed_columns c 
                           JOIN sys.tables t ON c.object_id=t.object_id 
                           JOIN sys.schemas s ON s.schema_id=t.schema_id 
                           WHERE t.name='{query.table_name}' AND s.name='dbo'
                              ) tmp ON ic.COLUMN_NAME=tmp.col 
                      WHERE ic.TABLE_NAME='{query.table_name}' AND ic.TABLE_SCHEMA='dbo'"""
            effect_rows, description, res, success = self._exe_no_query(sql, conn)
            if not res or not res[0]:
                self.printer.print_error_msg(f"{query.table_name} not found!")
                return
            header2, res1 = self.before_print(self._get_table_head_from_description(description[1]), res[1], query)
            header, res2 = self.before_print(self._get_table_head_from_description(description[0]), res[0], query)
            index_dict, foreign_dict = self.get_sqlserver_index_information_dict(conn, query.table_name)
            primary_key, mul_unique = [], {}
            for k, v in index_dict.items():
                if v[2] == 'PK':
                    primary_key.append(f'[{k}]')
                elif v[2] == 'UQ':
                    mul_unique[v[1]] = l = mul_unique.get(v[1], [])
                    l.append(f'[{k}]')
            res_list.append(f"CREATE TABLE [{res1[0][1]}].[{query.table_name}] (\n")
            for index, (row, row2) in enumerate(zip(res1, res2)):
                col_name, data_type = row2[3], row[4]
                if row[8]:
                    res_list.append(f"  [{col_name}] AS {row[8]}")
                else:
                    res_list.append(f"  [{col_name}] {data_type}")
                    if row[5] is not None and data_type not in ('text', 'ntext', 'xml'):
                        res_list.append(f"({'max' if row[5] == -1 else row[5]})")
                    elif data_type in ('decimal', 'numeric'):
                        res_list.append(f"({row[6]},{row[7]})")
                    if row2[5].endswith("identity"):
                        id_seed, id_incr = self._exe_no_query(
                            f"SELECT IDENT_SEED('{query.table_name}'),IDENT_INCR('{query.table_name}')",
                            conn)[2][0][0]
                        res_list.append(f" IDENTITY({id_seed},{id_incr})")
                    if row2[12] is not None:
                        res_list.append(f" DEFAULT {row2[12]}")
                    if row[3] == 'NO':
                        res_list.append(" NOT NULL")
                if index == len(res1) - 1:
                    res_list.append(f",\n  PRIMARY KEY({','.join(primary_key)})" if primary_key else '')
                    res_list.append(',\n' if mul_unique else '')
                    res_list.append(
                        ',\n'.join([f"  CONSTRAINT [{k}] UNIQUE({','.join(v)})" for k, v in mul_unique.items()]))
                    res_list.append('\n')
                else:
                    res_list.append(",\n")
            res_list.append(");")
            comment = self._exe_query(
                f"""SELECT col.name,CONVERT(varchar,ep.value),ep.name comment,CONVERT(varchar,SQL_VARIANT_PROPERTY(ep.value,'BaseType')) type,ep.minor_id 
                    FROM dbo.syscolumns col 
                    JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 
                    LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id
                    WHERE obj.name='{query.table_name}' AND ep.value is NOT NULL
                    UNION ALL 
                    SELECT obj.name,CONVERT(varchar,ep.value),ep.name comment,CONVERT(varchar,SQL_VARIANT_PROPERTY(ep.value,'BaseType')) type,ep.minor_id 
                    FROM dbo.sysobjects obj 
                    JOIN sys.extended_properties ep on obj.id=ep.major_id 
                    WHERE ep.minor_id=0 AND obj.xtype='U' AND obj.status>=0 AND obj.name='{query.table_name}'""",
                conn)[1]
            if comment:
                for com in comment[0]:
                    res_list.append(
                        f"""\nEXEC sp_addextendedproperty '{com[2]}' , {com[1] if is_number(com[3]) else "'{}'".format(com[1])}, 'SCHEMA', '{res1[0][1]}', 'TABLE', '{query.table_name}';""" if
                        com[4] == 0 else
                        f"""\nEXEC sp_addextendedproperty '{com[2]}' , {com[1] if is_number(com[3]) else "'{}'".format(com[1])}, 'SCHEMA', '{res1[0][1]}', 'TABLE', '{query.table_name}', 'COLUMN', '{com[0]}';""")
            for k, v in foreign_dict.items():
                res_list.append(
                    f'\nALTER TABLE [{res1[0][1]}].[{query.table_name}] WITH CHECK ADD CONSTRAINT [{v[1]}] FOREIGN KEY([{k}]) REFERENCES [{res1[0][1]}].[{v[3]}] ([{v[4]}]);\n')
                res_list.append(f'ALTER TABLE [{res1[0][1]}].[{query.table_name}] CHECK CONSTRAINT [{v[1]}];')
            return ''.join(res_list)

        if query.server_type is DatabaseType.MYSQL:
            return get_create_table_mysql_ddl()
        if query.server_type is DatabaseType.SQL_SERVER:
            return get_create_table_sql_server_ddl()

    def _print_table_schema(self, conn, query, attach_sql=False):
        if query.server_type is DatabaseType.MONGO:
            self.printer.print_error_msg("Not Support MongoDB!")
            self.write_error_history("Not Support MongoDB")
            return
        if self.out_format != 'sql':
            self.print_table_description(conn, query)
            if not attach_sql:
                return
        if self.out_format in {'sql', 'markdown'}:
            ddl = self.get_create_table_ddl(conn, query)
            self.printer.output(f'\n```sql\n{ddl}\n```' if self.out_format == 'markdown' else ddl)

    def exe(self):
        tab_name, server = self.params['option_val'], self.get_server()
        conn, dc = server.get_connection(), server.db_conf
        self._print_table_schema(conn, Query(dc.server_type, dc.database, None, tab_name, self.fold,
                                             self.columns, self.limit_rows, self.human))
        conn.close()
