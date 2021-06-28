from base.query_functions import process
import logging

logging.basicConfig(filename='test_log.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_process():
    logging.debug("testing process function")
    data = [
        {
            "page": 1,
            "pages": 6,
            "per_page": "50",
            "total": 297
        },
        [
            {
                "id": "ABW",
                "iso2Code": "AW",
                "name": "Aruba",
                "region": {
                    "id": "LCN",
                    "iso2code": "ZJ",
                    "value": "Latin America & Caribbean "
                },
                "adminregion": {
                    "id": "",
                    "iso2code": "",
                    "value": ""
                },
                "incomeLevel": {
                    "id": "HIC",
                    "iso2code": "XD",
                    "value": "High income"
                },
                "lendingType": {
                    "id": "LNX",
                    "iso2code": "XX",
                    "value": "Not classified"
                },
                "capitalCity": "Oranjestad",
                "longitude": "-70.0167",
                "latitude": "12.5167"
            }
        ]
    ]

    returned_dict = process(data).to_dict("records")

    expected = [
        {
            "id": "ABW",
            "iso2Code": "AW",
            "name": "Aruba",
            "capitalCity": "Oranjestad",
            "longitude": "-70.0167",
            "latitude": "12.5167",
            "region.id": "LCN",
            "region.iso2code": "ZJ",
            "region.value": "Latin America & Caribbean ",
            "adminregion.id": "",
            "adminregion.iso2code": "",
            "adminregion.value": "",
            "incomeLevel.id": "HIC",
            "incomeLevel.iso2code": "XD",
            "incomeLevel.value": "High income",
            "lendingType.id": "LNX",
            "lendingType.iso2code": "XX",
            "lendingType.value": "Not classified"
        }
    ]

    assert returned_dict == expected
    logger.info('executed test process')


if __name__ == "__main__":
    logging.info('test started')
    test_process()
    logging.info('test completed')