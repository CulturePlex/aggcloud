# -*- coding: utf-8 -*-
from cli import SylvaApp
import imp
from gooey import Gooey, GooeyParser
try:
    import ujson as json
except ImportError:
    import json  # NOQA
import os

APP_ROOT = os.path.dirname(__file__)
RULES_PATH = os.environ.get("RULES_PATH",
                            os.path.join(APP_ROOT, "rules.py"))
rules = imp.load_source('rules', RULES_PATH)

running = True


@Gooey(dump_build_config=True,
       program_name="SylvaDB - client")
def main():
    settings_msg = rules.CONFIG_SETTINGS['settings_msg']
    file_help_msg = rules.CONFIG_SETTINGS['file_help_msg']

    parser = GooeyParser(description=settings_msg)
    parser.add_argument("FileChooser", help=file_help_msg,
                        widget="FileChooser")

    args = parser.parse_args()

    file_path = args.FileChooser.encode('utf-8')

    app = SylvaApp(file_path)
    app.populate_data()

if __name__ == '__main__':
    main()
