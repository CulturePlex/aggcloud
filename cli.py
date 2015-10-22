# -*- coding: utf-8 -*-
try:
    import ujson as json
except ImportError:
    import json  # NOQA

from app import SylvaApp
from gooey import Gooey, GooeyParser
from rules import *

running = True


@Gooey(dump_build_config=True,
       program_name="SylvaDB - client")
def main():
    settings_msg = CONFIG_SETTINGS['settings_msg']
    file_help_msg = CONFIG_SETTINGS['file_help_msg']

    parser = GooeyParser(description=settings_msg)
    parser.add_argument("FileChooser", help=file_help_msg,
                        widget="FileChooser")

    args = parser.parse_args()

    file_path = args.FileChooser.encode('utf-8')

    app = SylvaApp(file_path)
    app.populate_data()

if __name__ == '__main__':
    main()
