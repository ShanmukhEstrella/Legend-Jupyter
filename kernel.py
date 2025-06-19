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
from IPython.display import Audio
import numpy as np
import io
import wave
import base64

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
                'text': f'\033[92mLoading csv data into table in DuckDB connection... {elapsed:.2f} seconds elapsed\n\033[0m'
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
                 }
                self.send_response(self.iopub_socket, 'stream', stream_content)
                # Generate a 1-second sine wave (440 Hz)
                rate = 44100
                t = np.linspace(0, 1, rate, False)
                data = (np.sin(2 * np.pi * 440 * t) * 32767).astype(np.int16)
                # Encode as WAV in memory
                buffer = io.BytesIO()
                with wave.open(buffer, 'wb') as wf:
                    wf.setnchannels(1)
                    wf.setsampwidth(2)
                    wf.setframerate(rate)
                    wf.writeframes(data.tobytes())
                    # Base64 encode
                    wav_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                # Build display content
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
                # Send it to the Jupyter frontend
                self.send_response(self.iopub_socket, 'display_data', display_content)
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
                'text': f"\033[92mExploring DataBase... {elapsed:.2f} seconds elapsed\n\033[0m"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
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
                'text': f"\033[92mDropping all tables... {elapsed:.2f} seconds elapsed\n\033[0m"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
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
        elif code.startswith("macro "):
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
                'text': f"\033[92mSetting up macro... {elapsed:.2f} seconds elapsed\n\033[0m"
                    }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
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
        elif code.startswith("show_macros"):
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
                'text': f"\033[92mRetrieving macros... {elapsed:.2f} seconds elapsed\n\033[0m"
                    }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
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
        elif code.startswith("clear_macros"):
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
                'text': f"\033[92mClearing macros... {elapsed:.2f} seconds elapsed\n\033[0m"
                    }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[0m"
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
                'text': f"\033[92mData Quereying... {elapsed:.2f} seconds elapsed\n\033[0m"
                 }
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                    time.sleep(0.01)
                stream_content = {
                'name': 'stdout',
                'text': f"\033[92mTotal Time Taken - {elapsed:.2f}s\n\033[2m"
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
        # 1. Suggest keyword if only starting to type (first word)
        if len(tokens) == 1 and not code_upto_cursor.endswith(" "):
            matches = [kw + ' ' for kw in keywords if kw.startswith(token)]
            return {
                'status': 'ok',
                'matches': matches,
                'cursor_start': cursor_pos - len(token),
                'cursor_end': cursor_pos,
                'metadata': {},
            }
        # 2. Check for file path suggestion (after `load` or `db`)
        if tokens and tokens[0] in {"load", "db"}:
            match = re.search(r'(["\']?)(?P<path>(?:~|\.{0,2}/|/)[^\'"\s]*)$', code_upto_cursor)
            if match:
                raw_path = match.group("path")
                base_dir = os.path.dirname(os.path.expanduser(raw_path)) or "."
                prefix = os.path.basename(raw_path)
                try:
                    entries = os.listdir(base_dir)
                except Exception:
                    entries = []

                matches = []
                for entry in entries:
                    if entry.startswith(prefix):
                        full_path = os.path.join(base_dir, entry)
                        matches.append(entry + ("/" if os.path.isdir(full_path) else " "))
                return {
                    "status": "ok",
                    "matches": matches,
                    "cursor_start": cursor_pos - len(prefix),
                    "cursor_end": cursor_pos,
                    "metadata": {}
                }
            # 3. If path ends in space and file exists, suggest connections
            if os.path.exists(tokens[-1]) and code_upto_cursor.endswith(" "):
                matches = [c + ' ' for c in connection_names]
                return {
                    'status': 'ok',
                    'matches': matches,
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


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=LegendPureKernel)
