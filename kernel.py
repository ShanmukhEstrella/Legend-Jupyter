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
        'name': 'python',
        'mimetype': 'text/x-pure',
        'file_extension': '.pure',
        'pygments_lexer': 'python'
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
        elif code.startswith("load "):
            import threading, time
            from IPython.display import clear_output
            # Setup stop event for timing thread
            stop_event = threading.Event()
            # Live timer function
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    stream_content = {
                'name': 'stdout',
                'text': f"Loading csv data into table in DuckDB connection... {elapsed:.2f} seconds elapsed\n"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"Total Time Taken - {elapsed:.2f}s\n"
                 }
                self.send_response(self.iopub_socket, 'stream', stream_content)
            # Start the timer thread
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            # Make the API request
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            # Show result
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)

            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("db"):
            import threading, time
            from IPython.display import clear_output
            # Setup stop event for timing thread
            stop_event = threading.Event()
            # Live timer function
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    stream_content = {
                'name': 'stdout',
                'text': f"Exploring DataBase... {elapsed:.2f} seconds elapsed\n"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"Total Time Taken - {elapsed:.2f}s\n"
                 }
                self.send_response(self.iopub_socket, 'stream', stream_content)
            # Start the timer thread
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            # Make the API request
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            # Show result
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)

            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        elif code.startswith("drop_all_tables"):
            import threading, time
            from IPython.display import clear_output
            # Setup stop event for timing thread
            stop_event = threading.Event()
            # Live timer function
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    stream_content = {
                'name': 'stdout',
                'text': f"Dropping all tables... {elapsed:.2f} seconds elapsed\n"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"Total Time Taken - {elapsed:.2f}s\n"
                 }
                self.send_response(self.iopub_socket, 'stream', stream_content)
            # Start the timer thread
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            # Make the API request
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            # Show result
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
            import threading, time
            from IPython.display import clear_output
            # Setup stop event for timing thread
            stop_event = threading.Event()
            # Live timer function
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    stream_content = {
                'name': 'stdout',
                'text': f"Data Quereying... {elapsed:.2f} seconds elapsed\n"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"Total Time Taken - {elapsed:.2f}s\n"
                 }
                self.send_response(self.iopub_socket, 'stream', stream_content)
            # Start the timer thread
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            # Make the API request
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            # Show result
            output = response.json()
            try:
                df = pandas.DataFrame(output) 
            except Exception:
                output = response.text
                stream_content = {'name': 'stderr', 'text': output[1:len(output)-1]}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
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
            'ext', 'loadProject', 'loadSnowflakeConnection ', 'drop_all_tables ','local::DuckDuckConnection',
            'exploreSchemaFromConnection ', 'createStoreFromConnectionTable ', '#>{'
        ]

        duck_conn_suggestion = 'local::DuckDuckConnection'
        code_upto_cursor = code[:cursor_pos]
        token = code_upto_cursor.split()[-1] if code_upto_cursor.split() else ''
        stripped_code = code_upto_cursor.strip()

        # ------------------ PATH COMPLETION ------------------
        fs_match = re.search(r'(["\']?)(?P<path>(?:~|\.{0,2}/|/)[^\'"\s]*)$', code_upto_cursor)
        if fs_match:
            raw_path = fs_match.group("path")
            base_dir = os.path.expanduser(os.path.dirname(raw_path) or ".")
            prefix = os.path.basename(raw_path)

            try:
                entries = os.listdir(base_dir)
            except Exception:
                entries = []

            matches = []
            for e in entries:
                if e.startswith(prefix) and e != prefix:
                    full_path = os.path.join(base_dir, e)
                    if os.path.isdir(full_path):
                        e += "/"
                    matches.append(e)

            # Suggest only the suffix (not full path) for relative suggestions
            cursor_start = cursor_pos - len(prefix)
            cursor_end = cursor_pos

            return {
                "status": "ok",
                "matches": matches,
                "cursor_start": cursor_start,
                "cursor_end": cursor_end,
                "metadata": {}
            }

         # -------- Fallback to keyword completion --------
        # Don't trigger keyword suggestions if the last token is empty (after space)
        if token in keywords or token + " " in keywords:
            # Already typed full keyword, no need to suggest
            return {
                'status': 'ok',
                'matches': [],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
            }

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
