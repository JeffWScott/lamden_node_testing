from contracting.client import ContractDriver, ContractingClient
from lamden.cli.start import start_node, join_network
from lamden.contracts import sync
from lamden.storage import BlockStorage


def flush(args):
    if args.storage_type == 'blocks':
        BlockStorage().flush()
    elif args.storage_type == 'state':
        ContractDriver().flush()
    elif args.storage_type == 'all':
        BlockStorage().flush()
        ContractDriver().flush()
    else:
        print('Invalid option. < blocks | state | all >')



def main():
    parser = argparse.ArgumentParser(description="Lamden Commands", prog='lamden')
    setup_cilparser(parser)
    args = parser.parse_args()

    # implementation
    if vars(args).get('command') is None:
        return

    if args.command == 'start':
        start_node(args)

    elif args.command == 'flush':
        flush(args)

    elif args.command == 'join':
        join_network(args)

    elif args.command == 'sync':
        client = ContractingClient()
        sync.flush_sys_contracts(client=client)
        sync.submit_from_genesis_json_file(client=client)

if __name__ == '__main__':
    main()
