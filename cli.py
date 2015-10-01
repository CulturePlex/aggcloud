from gooey import Gooey, GooeyParser

running = True


@Gooey(optional_cols=2,
       dump_build_config=True,
       program_name="SylvaDB - client")
def main():
    settings_msg = 'SylvaDB Interface'
    parser = GooeyParser(description=settings_msg)

    parser.add_argument('--verbose', help='be verbose', dest='verbose',
                        action='store_true', default=False)
    subs = parser.add_subparsers(help='commands', dest='command')

    get_parser = subs.add_parser('get', help=(
        'Using this option, we will be able to get data. We need to check ' +
        'the options to show.'))
    get_parser.add_argument("-op1", "--option1", action="store_true",
                            help="Option 1 information")
    get_parser.add_argument("-op2", "--option2", action="store_true",
                            help="Option 2 information")
    get_parser.add_argument("-op3", "--option3", action="store_true",
                            help="Option 3 information")
    get_parser.add_argument("-op4", "--option4", action="store_true",
                            help="Option 4 information")

    post_parser = subs.add_parser('post', help=(
        'Using this option, we will be able to set up our elements.'))
    post_parser.add_argument('Element', action='count',
                             help="Select element")
    post_parser.add_argument('Option1', help='')
    post_parser.add_argument('Option2', help='')
    post_parser.add_argument('Option3', help='')
    post_parser.add_argument('Option4', help='')

    args = parser.parse_args()

    print args

if __name__ == '__main__':
    main()
