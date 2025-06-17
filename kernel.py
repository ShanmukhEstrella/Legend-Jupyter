import ast
import requests
import datetime
import json
from ipykernel.kernelbase import Kernel
from .magics import CELL_MAGICS, LINE_MAGICS

class LegendPureKernel(Kernel):
    implementation = 'LegendPureKernel'
    implementation_version = '0.1'
    language = 'pure'
    language_version = '1.0'
    language_info = {
        'name': 'pure',
        'mimetype': 'text/x-pure',
        'file_extension': '.pure',
    }
    banner = "FINOS Legend Kernel for Jupyter (via REST API)"
    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        magic_line, *cell_lines = code.splitlines()
        cell_code = "\n".join(cell_lines)
        if code.startswith("%%"):
            magic_name = magic_line[2:]
            if magic_name in CELL_MAGICS:
                output = CELL_MAGICS[magic_name](cell_code)
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                } 
        elif code.startswith("%"):
            magic_line = code.strip().split()
            magic_name = magic_line[0][1:].strip()
            if magic_name == 'date':
                t = datetime.datetime.now()
                t = t.strftime("%Y-%m-%d %H:%M:%S")
                stream_content = {'name': 'stdout', 'text': t}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            elif magic_name in LINE_MAGICS:
                output = LINE_MAGICS[magic_name](magic_line[1])
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
        elif code.startswith("sql_to_json_line"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/sql/v1/grammar/grammarToJson",data=cell_code, headers=headers)
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("sql_to_json_batch"):
            headers = {"Content-Type": "application/json"}
            queries = [q.strip() for q in cell_code.split(";") if q.strip()]
            payload ={
                f"query{i+1}": {"value": query + ";"}
                for i, query in enumerate(queries)
            }
            response = requests.post(
                "http://127.0.0.1:9095/api/sql/v1/grammar/grammarToJson/batch",
                data=json.dumps(payload),
                headers=headers
            )
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("show_func_activators"):
            headers = {"Content-Type": "text/plain"}
            response = requests.get("http://127.0.0.1:9095/api/functionActivator/list")
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }    
        elif code.startswith("pure_compile"):
            headers = {"Content-Type": "application/json"}
            payload = {"code": cell_code}
            response = requests.post("http://127.0.0.1:9095/api/pure/v1/compilation/compile",data=json.dumps(payload), headers=headers)
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("sql_execute_line"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/sql/v1/execution/executeQueryString",data=cell_code, headers=headers)
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("get_schema_sql_line"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/sql/v1/execution/getSchemaFromQueryString",data=cell_code, headers=headers)
            output = response.json()
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("create "):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/createtable",data=magic_line, headers=headers)
            output = response.json()
            s='-----------------Table Created-----------------'+'\n' + "-----Ware House: "+output["warehouse"]+"-----"+'\n' + "-----DataBase: "+output["database"]+"-----"+'\n' + "-----Schema: "+output["schema"]+"-----"+'\n'
            s = s+ "-----Columns-----"+'\n'
            p = output["columns"]
            for column in p:
                s = s + column["name"] + f"[{column["type"]}]"
                if(column["primaryKey"]==True):
                    s = s + " is a Primary Key"+"\n"
                else:
                    s = s+'\n'
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("insertrow"):
            headers = {"Content-Type": "application/json"}
            if '->' not in cell_code:
                raise ValueError("Missing '->' separator in input")
            row_part, path_part = cell_code.split('->', 1)
            row_part = row_part.strip()
            path_part = path_part.strip()
            if not (row_part.startswith('[') and row_part.endswith(']')):
                raise ValueError("Row part must be enclosed in [ ]")
            row_part = row_part[1:-1].strip()
            row_dict = {}
            for pair in row_part.split(','):
                if ':' not in pair:
                    raise ValueError(f"Invalid entry: {pair}")
                key, val = map(str.strip, pair.split(':', 1))
                try:
                    parsed_val = ast.literal_eval(val)
                except Exception:
                    parsed_val = val
                row_dict[key] = parsed_val
            payload = {"path": path_part,"row": row_dict}
            response = requests.post("http://127.0.0.1:9095/api/data/insertrow",data=json.dumps(payload), headers=headers)
            output = response.json()
            if "error" in output:
                self.send_response(self.iopub_socket, 'stream', {
                'name': 'stderr',
                'text': output["error"]
                })
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'ename': 'ExecutionError',
                    'evalue': output["error"],
                    'traceback': [output["error"]],
                }
            else:
                s = 'Row Added.' + '\n' + '('
                p = output["row"]
                for fields in p:
                    s = s +  f"{fields}: {p[fields]}"  + ', '
                s = s[0:len(s)-2] + ')'
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
        elif code.startswith("delete_row"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/deleterow", data=cell_code, headers=headers)
            output = response.json()
            if "error" in output:
                self.send_response(self.iopub_socket, 'stream', {
                'name': 'stderr',
                'text': output["error"]
                })
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'ename': 'ExecutionError',
                    'evalue': output["error"],
                    'traceback': [output["error"]],
                }
            deleted = output["deletedRow"]
            s = 'Row deleted successfully:\n('
            for key, val in deleted.items():
                s += f"{key}: {val}, "
            s = s.rstrip(", ") + ')'
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("show_table"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/fetchtable",data=cell_code,headers=headers)
            output = response.json()
            import pandas as pd
            if "error" in output:
                self.send_response(self.iopub_socket, 'stream', {
                'name': 'stderr',
                'text': output["error"]
                })
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'ename': 'ExecutionError',
                    'evalue': output["error"],
                    'traceback': [output["error"]],
                }
            df = pd.DataFrame(output)
            display_content = {
                'data': {
                    'text/plain': str(df),
                    'text/html': df.to_html()
                },
                'metadata': {}
            }
            self.send_response(self.iopub_socket, 'display_data', display_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("show_all_tables"):
            response = requests.get("http://127.0.0.1:9095/api/data/showtables")
            output = response.json()
            s = (f"Number of tables: {output['count']}") + "\n"
            s = s + "Tables:" + "\n"
            for table in output['tables']:
                s = s + table + '\n'
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("load duckdb"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/duckdb/load",data=cell_code, headers=headers)
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("query duckdb"):
            headers = {"Content-Type": "application/json"}
            magic_line_new = magic_line.split()
            payload = {
                "dbPath": f"{magic_line_new[2]}",
                "query": f"{cell_code}"
            }
            response = requests.post("http://127.0.0.1:9095/api/data/duckdb/query", data=json.dumps(payload), headers=headers)
            output = response.json()
            import pandas as pd
            if "error" in output:
                self.send_response(self.iopub_socket, 'stream', {
                'name': 'stderr',
                'text': output["error"]
                })
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'ename': 'ExecutionError',
                    'evalue': output["error"],
                    'traceback': [output["error"]],
                }
            df = pd.DataFrame(output)
            display_content = {
                'data': {
                    'text/plain': str(df),
                    'text/html': df.to_html()
                },
                'metadata': {}
            }
            self.send_response(self.iopub_socket, 'display_data', display_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            } 
        elif code.startswith("load ") or code.startswith("db") or code.startswith("drop_all_tables "):
            response = requests.post("http://127.0.0.1:9095/api/server/execute",json={"line":magic_line});
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        else:
            import pandas
            response = requests.post("http://127.0.0.1:9095/api/server/execute",json={"line":magic_line});
            output = response.json()
            if("error" in output):
                self.send_response(self.iopub_socket, 'stream', {
                'name': 'stderr',
                'text': "Syntax error:" + output["error"]
                })
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'ename': 'ExecutionError',
                    'evalue': output["error"],
                    'traceback': [output["error"]],
                }
            df = pandas.DataFrame(output) 
            display_content = {
                'data': {
                    'text/plain': str(df),
                    'text/html': df.to_html()
                },
                'metadata': {}
            }
            self.send_response(self.iopub_socket, 'display_data', display_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
    def do_complete(self, code, cursor_pos):
        import os
        import re
        keywords = [
            'createtable', 'sql_to_json_line', 'sql_to_json_batch',
            'show_func_activators', 'sql_execute_line', 'sql_execute_batch',
            'pure_compile', 'insertrow', 'show_all_tables', 'get_schema_sql_line',
            'db ', 'load ', 'cache ', 'graph ', 'show', 'showInAgGrid ',
            'ext', 'loadProject', 'loadSnowflakeConnection ', 'drop_all_tables ',
            'exploreSchemaFromConnection ', 'createStoreFromConnectionTable ', '#>{'
        ]
        duck_conn_suggestion = ' local::DuckDuckConnection'
        base_path = "/home/shannu/Documents/CSV/"
        code_upto_cursor = code[:cursor_pos]
        token = code_upto_cursor.split()[-1] if code_upto_cursor.split() else ''
        stripped_code = code_upto_cursor.strip()
        match_load = re.match(r'^load\s*(.*)$', stripped_code)
        if match_load:
            typed_path = match_load.group(1).strip()
            if typed_path == '' or not typed_path.endswith('.csv'):
                full_prefix = os.path.join(base_path, typed_path)
                dir_path = os.path.dirname(full_prefix) if os.path.dirname(full_prefix) else base_path

                try:
                    files = os.listdir(dir_path)
                    matches = [
                        os.path.join(dir_path, f) for f in files
                        if f.startswith(os.path.basename(full_prefix)) and f.endswith('.csv')
                    ]
                except FileNotFoundError:
                    matches = []
                return {
                    'status': 'ok',
                    'matches': matches,
                    'cursor_start': cursor_pos - len(typed_path),
                    'cursor_end': cursor_pos,
                    'metadata': {},
                }
            elif typed_path.endswith('.csv') and duck_conn_suggestion not in stripped_code:
                return {
                    'status': 'ok',
                    'matches': [duck_conn_suggestion],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                }
        match_db = re.match(r'^db\s*$', stripped_code)
        if match_db and duck_conn_suggestion not in stripped_code:
            return {
                'status': 'ok',
                'matches': [duck_conn_suggestion[1:]],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
            }
        match_drop = re.match(r'^drop_all_tables\s*$', stripped_code)
        if match_drop and duck_conn_suggestion not in stripped_code:
            return {
                'status': 'ok',
                'matches': [duck_conn_suggestion[1:]],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
            }

        # Default: keyword completions
        matches = [kw for kw in keywords if kw.startswith(token)]
        return {
            'status': 'ok',
            'matches': matches,
            'cursor_start': cursor_pos - len(token),
            'cursor_end': cursor_pos,
            'metadata': {},
        }


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=LegendPureKernel)
