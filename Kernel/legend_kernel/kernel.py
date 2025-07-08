import ast
import requests
import datetime
import json
import html
from ipykernel.kernelbase import Kernel
from .magics import CELL_MAGICS, LINE_MAGICS
from inspect import getmembers, isclass
from ipykernel.kernelbase import Kernel
import pandas as pd
import os
import re
from ipyaggrid import Grid
from IPython.display import display,HTML


class LegendKernel(Kernel):
    kernel_name = 'legend_kernel'
    implementation = kernel_name
    implementation_version = '1.0'
    language_info = {
        'name': 'legend',
        'file_extension': '.lgd',
        'mimetype': 'text/x-legend',
        'codemirror_mode': 'legend',
    }
    banner = kernel_name
    tables = []
    details = {}
    check = False



    def get_columns(self):
        if self.tables == []:
            return
        for x in self.tables:
            response = requests.post("http://127.0.0.1:9095/api/server/execute",json={"line":"get_attributes " + "local::DuckDuckConnection."+x})
            output = response.json()
            self.details[x] = [y for y in output["attributes"]]


    def initiate(self):
        from IPython.display import HTML
        import threading, time
        from IPython.display import clear_output
        stop_event = threading.Event()
        def show_running_time():
            start = time.time()
            while not stop_event.is_set():
                elapsed = time.time() - start
                s = HTML(f"<div style='color:  green;'>Kernel Warming up... {elapsed:.2f} seconds elapsed\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                time.sleep(0.01)
        timer_thread = threading.Thread(target=show_running_time)
        timer_thread.start()
        try:
            response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
            output = response.json()
            self.tables = [x for x in output["tables"]]
            self.get_columns()
        finally:
            stop_event.set()
            timer_thread.join()




    def parse_db_output(self, text: str):
        lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
        result = {
            'database': None,
            'tables': []
        }
        current_table = None
        inside_table = False
        for line in lines:
            if line.startswith("Database"):
                result['database'] = line.split("Database", 1)[1].strip()
            elif line.startswith("Table"):
                table_name = line.split("Table", 1)[1].strip()
                current_table = {'name': table_name, 'columns': []}
                inside_table = True
            elif line == ')':
                if inside_table and current_table:
                    result['tables'].append(current_table)
                    current_table = None
                    inside_table = False
            elif inside_table:
                if ',' in line:
                    line = line[:-1]  # remove trailing comma
                if ' ' in line:
                    col_name, col_type = line.split(None, 1)
                    current_table['columns'].append({'name': col_name, 'type': col_type})
        return result
    








    def render_database_ui(self,data):
        db_name = data.get("database", "Unknown")
        tables = data.get("tables", [])

        html_parts = [f"<details open><summary><b>Database: {html.escape(db_name)}</b></summary><div style='margin-left: 20px;'>"]

        for table in tables:
            table_name = table.get("name", "Unnamed Table")
            columns = table.get("columns", [])
            html_parts.append(f"<details><summary><b>Table: {html.escape(table_name)}</b></summary><div style='margin-left: 20px;'>")

            # Table of columns
            html_parts.append("<table border='1' style='border-collapse: collapse;'>")
            html_parts.append("<tr><th>Column Name</th><th>Type</th></tr>")
            for col in columns:
                col_name = html.escape(col.get("name", ""))
                col_type = html.escape(col.get("type", ""))
                html_parts.append(f"<tr><td>{col_name}</td><td>{col_type}</td></tr>")
            html_parts.append("</table></div></details>")

        html_parts.append("</div></details>")
        return "\n".join(html_parts)
    





    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if(self.check == False):
            self.initiate()
            self.check = True
        magic_line, *cell_lines = code.splitlines()
        cell_code = "\n".join(cell_lines)



        if code.strip().startswith("start_legend"):
            import threading, time
            from IPython.display import clear_output
            from IPython.display import HTML
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Activating Legend Features... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Legend Features Activated in - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            connection_name = "local::DuckDuckConnection"
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + connection_name})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            self.tables = [x for x in output["tables"]]
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }





        elif code.strip().startswith("%%"):
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
            



            
        elif code.strip().startswith("%"):
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
            



            
        elif code.strip().startswith("sql_to_json_line"):
            from IPython.display import HTML
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/sql/v1/grammar/grammarToJson",data=cell_code, headers=headers)
            output = response.json()
            if("code" in output and output["code"]==-1):
                s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        




        elif code.strip().startswith("sql_to_json_batch"):
            from IPython.display import HTML
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
            if("code" in output and output["code"]==-1):
                s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        



        elif code.strip().startswith("show_func_activators"):
            from IPython.display import HTML
            headers = {"Content-Type": "text/plain"}
            response = requests.get("http://127.0.0.1:9095/api/functionActivator/list")
            output = response.json()
            if("code" in output and output["code"]==-1):
                s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        



        elif code.strip().startswith("create "):
            from IPython.display import HTML
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/createtable",data=magic_line, headers=headers)
            output = response.json()
            if("error" in output):
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
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
        





        
        elif code.strip().startswith("insertrow"):
            from IPython.display import HTML
            headers = {"Content-Type": "application/json"}
            if '->' not in cell_code:
                s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            row_part, path_part = cell_code.split('->', 1)
            row_part = row_part.strip()
            path_part = path_part.strip()
            if not (row_part.startswith('[') and row_part.endswith(']')):
                s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            row_part = row_part[1:-1].strip()
            row_dict = {}
            for pair in row_part.split(','):
                if ':' not in pair:
                    s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
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
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
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
            









        elif code.strip().startswith("delete_row"):
            from IPython.display import HTML
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/deleterow", data=cell_code, headers=headers)
            output = response.json()
            if "error" in output:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
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
        









        elif code.strip().startswith("show_table"):
            from IPython.display import HTML
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/fetchtable",data=cell_code,headers=headers)
            output = response.json()
            if "error" in output:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            try:
                df = pd.DataFrame(output)
            except Exception as e:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
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
        





        elif code.strip().startswith("show_all_tables"):
            from IPython.display import HTML
            response = requests.get("http://127.0.0.1:9095/api/data/showtables")
            output = response.json()
            if "error" in output:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
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
        






        elif code.strip().startswith("load duckdb"):
            from IPython.display import HTML
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
        







        elif code.strip().startswith("query duckdb"):
            from IPython.display import HTML
            headers = {"Content-Type": "application/json"}
            magic_line_new = magic_line.split()
            payload = {
                "dbPath": f"{magic_line_new[2]}",
                "query": f"{cell_code}"
            }
            response = requests.post("http://127.0.0.1:9095/api/data/duckdb/query", data=json.dumps(payload), headers=headers)
            output = response.json()
            try:
                df = pd.DataFrame(output)
            except Exception as e:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
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
        







        elif code.strip().startswith("load "):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Loading csv data into table in DuckDB connection... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )    
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                response2 = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
                output2 = response2.json()
                self.tables = [x for x in output2["tables"]]
                self.get_columns()
            finally:
                stop_event.set()
                timer_thread.join()
            if(response.headers.get('Content-Type') == 'application/json'):
                output = response.json()
                s = HTML(f"<div style='color: red;'>{output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        










        elif code.strip().startswith("db"):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Exploring Database... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            if(response.headers.get('Content-Type') == 'application/json'):
                output = response.json()
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            output = response.text
            try:
                structured = self.parse_db_output(output)
                html_str = self.render_database_ui(structured)
                self.send_response(self.iopub_socket, 'display_data', {
                    'data': {'text/html': html_str},
                    'metadata': {}
                })
                return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
                }
            except Exception as e:
                from IPython.display import HTML
                s = HTML(f"<div style='color: red;'>Parsing/Rendering failed: {html.escape(str(e))}</div>")
                self.send_response(self.iopub_socket, 'display_data', {
                    'data': {'text/html': str(s.data)},
                    'metadata': {}
                })
                return {
                'status': 'error',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
                 }
        











        elif code.strip().startswith("drop_all_tables"):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Dropping tables from Connection... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                response2 = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
                output2 = response2.json()
                self.tables = [x for x in output2["tables"]]
                self.get_columns()
            finally:
                stop_event.set()
                timer_thread.join()
            if(response.headers.get('Content-Type') == 'application/json'):
                output = response.json()
                s = HTML(f"<div style='color: red;'>{output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        














        elif code.strip().startswith("macro "):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Establishing Macro... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Macro Established in - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.text
            if(output.startswith("Invalid")):
                s = HTML(f"<div style='color: red;'>Error: {output}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        










        elif code.strip().startswith("show_macros"):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Fetching all Macros... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Macros fetched in- {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        













        
        elif code.strip().startswith("clear_macros"):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Clearing all Macros... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Macros cleared in- {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.text
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        




        elif code.strip().startswith("get_tables "):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Fetching tables... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Tables Fetched in - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            s = ""
            for x in output["tables"]:
                s = s+x+"\n"
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream',stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        



        
        elif code.strip().startswith("get_attributes "):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Fetching attributes... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Attributes Fetched in - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            s=""
            for x in output["attributes"]:
                s = s + x + "\n"
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream',stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        



        elif code.strip().startswith("get_all"):
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Fetching attributes... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Attributes Fetched in - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            s=""
            for x in output["connections"]:
                s = s + x + "\n"
            stream_content = {'name': 'stdout', 'text': s}
            self.send_response(self.iopub_socket, 'stream',stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }







        elif code.strip().startswith("#>"):
            code = code.replace('\n', '')
            match1 = re.search(r'(.*?)--file\s+(\w+)\s*$', code.strip())
            match2 = re.search(r'(.*?)--var\s+(\w+)\s*$', code.strip())
            if match1:
                query_part = match1.group(1).strip()
                filename = match1.group(2).strip()
                variable = None
            elif match2:
                query_part = match2.group(1).strip()
                variable = match2.group(2).strip()
                filename = None
            else:
                query_part = code.strip()
                variable = None
                filename = None
            from IPython.display import HTML
            import threading, time
            from IPython.display import clear_output
            stop_event = threading.Event()
            def show_running_time():
                start = time.time()
                while not stop_event.is_set():
                    elapsed = time.time() - start
                    s = HTML(f"<div style='color:  green;'>Data Quereying... {elapsed:.2f} seconds elapsed\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                s = HTML(f"<div style='color:  green;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
            timer_thread = threading.Thread(target=show_running_time)
            timer_thread.start()
            try:
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": query_part})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            try:
                df = pd.DataFrame(output)
            except Exception:
                s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            def df_to_styled_html(df: pd.DataFrame) -> str:
                import html
                if df.empty:
                    return "<p style='font-family: Inter, sans-serif; font-size: 16px;'><em>No data available</em></p>"
                escaped = df.applymap(lambda val: html.escape(str(val)))
                header_html = ''.join(f'<th>{html.escape(col)}</th>' for col in escaped.columns)

                body_html = ''
                for _, row in escaped.iterrows():
                    row_html = ''.join(f'<td>{val}</td>' for val in row)
                    body_html += f'<tr>{row_html}</tr>'
                return f"""
                <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
                <style>
                    .pretty-table-wrapper {{
                        font-family: 'Inter', sans-serif;
                        font-size: 14px;
                        margin-top: 20px;
                        overflow-x: auto;
                        width: 100%;
                        box-sizing: border-box;
                        text-align: left; /* Ensure wrapper aligns content to the left */
                    }}

                    .pretty-table {{
                        border-collapse: collapse;
                        width: max-content;
                        min-width: 100%;
                        table-layout: auto;
                        color: var(--text-color);
                        background-color: var(--bg-color);
                    }}

                    .pretty-table thead {{
                        background-color: var(--header-bg);
                    }}

                    .pretty-table th, .pretty-table td {{
                        padding: 10px 14px;
                        text-align: left;
                        font-weight: bold; /* Make headers bold */
                        border: 1px solid var(--border-color);
                        white-space: nowrap;
                        max-width: 400px;
                        overflow: hidden;
                        text-overflow: ellipsis;
                    }}

                    .pretty-table tbody tr:hover {{
                        background-color: var(--hover-bg);
                        transition: background-color 0.3s ease;
                    }}

                    :root {{
                        --bg-color: #ffffff;
                        --text-color: #1a1a1a;
                        --header-bg: #e3f2fd;
                        --border-color: #c0c0c0;
                        --hover-bg: #f1faff;
                    }}

                    @media (prefers-color-scheme: dark) {{
                        :root {{
                            --bg-color: #1e1e1e;
                            --text-color: #f0f0f0;
                            --header-bg: #223a5f;
                            --border-color: #444;
                            --hover-bg: #2f4f73;
                        }}
                    }}
                </style>

                <div class="pretty-table-wrapper">
                    <table class="pretty-table">
                        <thead><tr>{header_html}</tr></thead>
                        <tbody>{body_html}</tbody>
                    </table>
                </div>
                """
            if filename!=None:
                df.to_csv(filename,index=False)
                s = HTML(f"<div style='color:  green;'>DataFrame saved in the file - {filename}\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
                }
            elif variable!=None:
                s = HTML(f"<div style='color:  green;'>DataFrame stored in the variable - \"{variable}\"\n</div>")
                variable = df
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
                }
            else:
                html_output = df_to_styled_html(df)
                self.send_response(self.iopub_socket, 'display_data', {
                    'data': {
                        'text/html': html_output,
                        'text/plain': str(df)
                    },
                    'metadata': {}
                })
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }




    def do_complete(self, code, cursor_pos):
        suggestions = ["load", "db", "#>"]
        prefix = code[:cursor_pos]
        tokens = prefix.strip().split()
        if tokens and tokens[0] == "load" and prefix.rstrip() == "load":
            match = " ~/"
            start = len(prefix)
            return {
                'matches': [match],
                'cursor_start': start,
                'cursor_end': start,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.startswith("load ~/"):
            path_prefix = prefix[len("load "):]
            if path_prefix.endswith('/'):
                dir_path = os.path.expanduser(path_prefix)
                partial = ''
            else:
                dir_path, partial = os.path.split(os.path.expanduser(path_prefix))
            try:
                entries = os.listdir(dir_path)
            except Exception:
                entries = []
            matches = [e for e in entries if e.startswith(partial)]
            matches = [
                e + '/' if os.path.isdir(os.path.join(dir_path, e)) else e
                for e in matches
            ]
            if not matches:
                matches = entries
                matches = [
                    e + '/' if os.path.isdir(os.path.join(dir_path, e)) else e
                    for e in matches
                ]
            cursor_start = len("load ") + len(path_prefix) - len(partial)
            return {
                'matches': matches,
                'cursor_start': cursor_start,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.endswith("#>"):
            return {
                'matches': ["{local::DuckDuckDatabase."],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.endswith("db"):
            return {
                'matches': [" local::DuckDuckConnection"],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.endswith("]"):
            return {
                'matches': [")"],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.endswith("}#") or prefix.endswith(")"):
            return {
                'matches': ["->"],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith(" ->") or prefix.strip().endswith("->"):
            return {
                'matches': ["filter(", "groupBy(", "select(", "extend(","from(" , "pivot(", "asofjoin(", "join(", "distinct(","rename(", "concatenate(",
                            "sort(","size(","drop("],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("x|$x."):
            match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]+)}#", prefix)
            if match:
                result = match.group(1)
            p = self.details[result]
            return {
                'matches': p,
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("|"):
            if(cursor_pos>=2):
                var = code[cursor_pos-2]
                return {
                    'matches': ["$"+var+"."],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            else:
                return {
                    'matches': [],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
        if prefix.strip().endswith("~") or prefix.strip().endswith(",") or prefix.strip().endswith("[") or (prefix.strip().endswith(".") and cursor_pos>=3 and code[cursor_pos-3]=="$"):
            match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]+)}#", prefix)
            if match:
                result = match.group(1)
            p = self.details[result]
            return {
                'matches': p,
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("filter("):
            return {
                'matches': ["x|$x."],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("from("):
            return {
                'matches': ["local::DuckDuckRuntime)"],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("--"):
            return {
                'matches': ["file", "var"],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if prefix.strip().endswith("select("):
            return {
                'matches': ["~["],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        if "#>{local::DuckDuckDatabase" in prefix:
            match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]*)$", prefix)
            if match:
                typed = match.group(1)
                matches = [
                    table +  "}#"
                    for table in self.tables
                    if table.lower().startswith(typed.lower())
                ]
                cursor_start = cursor_pos - len(typed)
                cursor_end = cursor_pos

                return {
                    'matches': matches,
                    'cursor_start': cursor_start,
                    'cursor_end': cursor_end,
                    'metadata': {},
                    'status': 'ok'
                }
        matches = [s for s in suggestions if s.startswith(prefix)]
        # if not matches:
        #     matches = suggestions
        return {
            'matches': matches,
            'cursor_start': 0,
            'cursor_end': cursor_pos,
            'metadata': {},
            'status': 'ok'
        }








# if __name__ == '__main__':
#     from ipykernel.kernelapp import IPKernelApp
#     IPKernelApp.launch_instance(kernel_class=LegendKernel)
