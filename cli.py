# -*- coding: utf-8 -*-
from app import SylvaApp
from gooey import Gooey, GooeyParser

running = True


@Gooey(dump_build_config=True,
       program_name="SylvaDB - client")
def main():
    settings_msg = 'SylvaDB Interface'
    file_help_msg = "Name of the file you want to process"

    parser = GooeyParser(description=settings_msg)
    parser.add_argument("FileChooser", help=file_help_msg,
                        widget="FileChooser")

    args = parser.parse_args()

    file_path = args.FileChooser.encode('utf-8')

    app = SylvaApp(file_path)
    app.populate_data()

if __name__ == '__main__':
    main()
