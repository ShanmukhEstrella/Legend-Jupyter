import ast
import requests
import datetime
import json
from ipykernel.kernelbase import Kernel
from .magics import CELL_MAGICS, LINE_MAGICS
from pygments.lexers import _mapping
from pygments.lexer import Lexer
from pygments import lexers
import numpy as np
from IPython.display import HTML
import numpy as np
import io
import wave
import base64
from .pure_lexer import PureCustomLexer

def register_custom_pure_lexer():
    lexers.LEXERS['Pure'] = (
        'pure_lexer',
        'PureCustomLexer',
        ('pure',),
        ('*.pure',),
        ('text/x-pure',)
    )
    _mapping.LEXERS['Pure'] = (
        'pure_lexer',
        'PureCustomLexer',
        ('pure',),
        ('*.pure',),
        ('text/x-pure',)
    )
    lexers._lexer_cache.clear()  # Clear cache if needed
register_custom_pure_lexer()

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
        



        elif code.startswith("show_func_activators"):
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
        



        elif code.startswith("create "):
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
        





        
        elif code.startswith("insertrow"):
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
            









        elif code.startswith("delete_row"):
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
        









        elif code.startswith("show_table"):
            headers = {"Content-Type": "text/plain"}
            response = requests.post("http://127.0.0.1:9095/api/data/fetchtable",data=cell_code,headers=headers)
            output = response.json()
            import pandas as pd
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
        





        elif code.startswith("show_all_tables"):
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
        







        elif code.startswith("load "):
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
            finally:
                stop_event.set()
                timer_thread.join()
            rate = 44100
            t = np.linspace(0, 1, rate, False)
            data = (np.sin(2 * np.pi * 440 * t) * 32767).astype(np.int16)
            buffer = io.BytesIO()
            with wave.open(buffer, 'wb') as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(rate)
                wf.writeframes(data.tobytes())
                wav_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            html = f"""
            <audio autoplay>
            <source src="data:audio/wav;base64,{wav_base64}" type="audio/wav">
            Your browser does not support the audio element.
            </audio>
            """
            display_content = {
                "data": {
                    "text/html": html,
                    "text/plain": "Beep Sound"
                },
                "metadata": {}
            }
            self.send_response(self.iopub_socket, 'display_data', display_content)
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
        










        elif code.startswith("db"):
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
            stream_content = {'name': 'stdout', 'text': output}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            return {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
        














        elif code.startswith("macro "):
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
        










        elif code.startswith("show_macros"):
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
        













        
        elif code.startswith("clear_macros"):
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
        













        else:
            import pandas
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
                response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
            finally:
                stop_event.set()
                timer_thread.join()
            output = response.json()
            try:
                df = pandas.DataFrame(output) 
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
        import os, re
        keywords = [
            'createtable', 'sql_to_json_line', 'sql_to_json_batch',
            'show_func_activators', 'sql_execute_line', 'sql_execute_batch',
            'pure_compile', 'insertrow', 'show_all_tables', 'get_schema_sql_line',
            'db', 'load', 'cache', 'graph', 'show', 'showInAgGrid',
            'ext', 'loadProject', 'loadSnowflakeConnection', 'drop_all_tables',
            'exploreSchemaFromConnection', 'createStoreFromConnectionTable', '#>{'
        ]
        connection_names = ['local::DuckDuckConnection', 'local::H2Connection']
        code_upto_cursor = code[:cursor_pos]
        tokens = code_upto_cursor.strip().split()
        token = tokens[-1] if tokens else ''
        if tokens and tokens[0] in {'load', 'db'} and code_upto_cursor.endswith(" "):
            if len(tokens) >= 2:
                last_path = os.path.expanduser(tokens[1])
                if os.path.isfile(last_path):
                    return {
                        'status': 'ok',
                        'matches': connection_names,
                        'cursor_start': cursor_pos,
                        'cursor_end': cursor_pos,
                        'metadata': {},
                    }
            return {
                'status': 'ok',
                'matches': [],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
            }
        if len(tokens) == 1 and not code_upto_cursor.endswith(" "):
            matches = [kw + ' ' for kw in keywords if kw.startswith(token)]
            return {
                'status': 'ok',
                'matches': matches,
                'cursor_start': cursor_pos - len(token),
                'cursor_end': cursor_pos,
                'metadata': {},
            }
        if tokens and tokens[0] in {'load', 'db'}:
            path_match = re.search(r'(["\']?)(?P<path>(?:~|\.{0,2}/|/)[^\'"\s]*)$', code_upto_cursor)
            if path_match:
                raw_path = path_match.group("path")
                expanded_path = os.path.expanduser(raw_path)
                base_dir = os.path.dirname(expanded_path) or "."
                prefix = os.path.basename(expanded_path)
                try:
                    entries = os.listdir(base_dir)
                except Exception:
                    entries = []
                matches = []
                for entry in entries:
                    if entry.startswith(prefix):
                        full_entry = entry + ("/" if os.path.isdir(os.path.join(base_dir, entry)) else " ")
                        matches.append(full_entry)
                return {
                    'status': 'ok',
                    'matches': matches,
                    'cursor_start': cursor_pos - len(prefix),
                    'cursor_end': cursor_pos,
                    'metadata': {},
                }
        return {
            'status': 'ok',
            'matches': [],
            'cursor_start': cursor_pos,
            'cursor_end': cursor_pos,
            'metadata': {},
        }








if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=LegendPureKernel)
