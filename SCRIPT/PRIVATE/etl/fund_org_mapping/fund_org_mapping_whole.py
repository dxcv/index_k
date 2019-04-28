from SCRIPT.PRIVATE.etl.fund_org_mapping.fund_org_mapping_xfi import Streams
from SCRIPT.PRIVATE.etl.fund_org_mapping import fund_org_mapping_03and04


def main():
    Streams.clean()
    fund_org_mapping_03and04.main()

if __name__ == "__main__":
    main()
