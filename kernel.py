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
    banner = "FINOS Legend PURE Kernel for Jupyter (via REST API)"

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if code.startswith("%%"):
            magic_line, *cell_lines = code.splitlines()
            magic_name = magic_line[2:].strip()
            cell_code = "\n".join(cell_lines)
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
            elif magic_name == "sql":
                headers = {"Content-Type": "text/plain"}
                # stream_content = {'name': 'stdout', 'text': f"SQL Input Sent:\n{code}"}
                # self.send_response(self.iopub_socket, 'stream', stream_content)
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
            elif magic_name == "show_func_activators":
                headers = {"Content-Type": "text/plain"}
                # stream_content = {'name': 'stdout', 'text': f"SQL Input Sent:\n{code}"}
                # self.send_response(self.iopub_socket, 'stream', stream_content)
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
            elif magic_name == "pure":
                headers = {"Content-Type": "application/json"}
                payload = {"code": cell_code}
                # stream_content = {'name': 'stdout', 'text': f"SQL Input Sent:\n{code}"}
                # self.send_response(self.iopub_socket, 'stream', stream_content)
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
        
        # lint_error = self.lint_code(code)
        # if lint_error:
        #     error_content = {'name': 'stderr', 'text': f"Syntax Error: {lint_error}"}
        #     self.send_response(self.iopub_socket, 'stream', error_content)
        #     return {
        #         'status': 'error',
        #         'execution_count': self.execution_count,
        #         'ename': 'LintError',
        #         'evalue': lint_error,
        #         'traceback': []
        #     }

        # if not silent:
        #     try:
        #         output = self.run_legend_pure_code(code)
        #         stream_content = {'name': 'stdout', 'text': output}
        #         self.send_response(self.iopub_socket, 'stream', stream_content)
        #     except Exception as e:
        #         error_content = {'name': 'stderr', 'text': f"Error: {str(e)}"}
        #         self.send_response(self.iopub_socket, 'stream', error_content)

        # return {
        #     'status': 'ok',
        #     'execution_count': self.execution_count,
        #     'payload': [],
        #     'user_expressions': {}
        # }

    def run_legend_pure_code(self, code: str) -> str:
        url = ""  # Set to your Legend Engine endpoint
        headers = {"Content-Type": "application/json"}
        payload = {"code": code}

        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                return f"Legend Engine Error {response.status_code}:\n{response.text}"
            return response.text
        except requests.exceptions.RequestException as e:
            return f"Could not connect to Legend Engine: {str(e)}"


    def do_complete(self, code, cursor_pos):
        keywords = [
            'function', 'Class', 'let', 'if', 'else', 'true', 'false',
            'return', 'match', 'import', 'native', 'extends', 'package'
        ]
        token = code[:cursor_pos].split()[-1] if code[:cursor_pos].split() else ''
        matches = [kw for kw in keywords if kw.startswith(token)]
        return {
            'status': 'ok',
            'matches': matches,
            'cursor_start': cursor_pos - len(token),
            'cursor_end': cursor_pos,
            'metadata': {},
        }
    
    def lint_code(self, code: str) -> str:
        if not code.strip():
            return "Code cannot be empty"
        if not code.endswith(";"):
            return "Code must end with a semicolon" 
        return None
# Add this to launch the kernel when run as a module
if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=LegendPureKernel)
