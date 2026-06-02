import asyncio
import json
import os
import sys
import webbrowser
from contextlib import suppress
from pathlib import Path
from typing import Dict, Any

import pytest
from aiohttp.web_request import Request
from aiohttp.web_response import Response, StreamResponse

from hydra.nano_services.http import WebApplication


PORT = 8089


class TestJavascriptGenerator:

    @pytest.mark.asyncio
    async def test_generate_basic(self):
        def assert_preprocessor(request: Request) -> Dict[str, Any]:
            assert isinstance(request, Request), "Failed to get valid response on pre-processing"
            return {}

        def assert_postprocessor(response: Response) -> None:
            assert isinstance(response, (Response, StreamResponse)), "Failed to get valid response for post-processing"

        from class_js_test import RestAPIExample
        RestAPIExample.result_queue = asyncio.Queue()
        root = Path(__file__).parent
        static_path = root.joinpath('static')
        app = WebApplication(static_path=static_path, js_bundle_name='generated', using_async=False)
        app.set_preprocessor(assert_preprocessor)
        app.set_postprocessor(assert_postprocessor)

        async def launch_browser():
            await asyncio.sleep(2.0)
            default = False
            try:
                browser = webbrowser.get("chrome")
            except:
                with suppress(Exception):
                    browser = webbrowser.get("google-chrome")
                if not browser:
                    browser = webbrowser.get()
                    default = True
            flags = ["--new-window"] if browser and not default else []
            if not browser:
                with suppress(Exception):
                    browser = webbrowser.get("firefox")
                    flags = ["-new-instance"]
            if not browser:
                os.write(sys.stderr.fileno(),
                         b"UNABLE TO GET BROWSER SUPPORT HEADLESS CONFIGURATION. DEFAULTING TO NON_HEADLESSS")
                browser = webbrowser.get()
            browser.open(f"http://localhost:{PORT}/static/index.html")

        app_task = asyncio.create_task(app.start(modules=['class_js_test'], port=PORT))
        browser_task = asyncio.create_task(launch_browser())
        try:
            result = await asyncio.wait_for(RestAPIExample.result_queue.get(), timeout=120)
            if result != 'PASSED':
                await asyncio.sleep(2.0)
        except Exception as e:
            await asyncio.sleep(2.0)
            assert False, f"Exception processing javascript results: {e}"
        finally:
            await app.shutdown()
            browser_task.cancel()

        if isinstance(result, Exception):
            assert False, "At least one javascript test failed. See browser window for details"
        assert result == "PASSED", \
            "FAILED JAVASCRIPT TESTS FOUND: \n" + \
            "\n".join([f"{test}: {msg}" for test, msg in json.loads(result).items()])
