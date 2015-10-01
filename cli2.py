import unicodecsv
import StringIO
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

    # We load the csv file into a pandas dataframe
    f = StringIO.StringIO(args.FileChooser.encode('utf8'))
    r = unicodecsv.reader(f, encoding='utf-8')
    print r.next()

if __name__ == '__main__':
    main()
