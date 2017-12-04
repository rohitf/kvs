import functions as fn
from globals import *


def test_metadata():
    METADATA.ewfe = 7
    METADATA.ID = 5
    print (vars(METADATA))

test_metadata()
