import functions as fn
from globals import *


def test_metadata():
    META.ewfe = 7
    META.ID = 5
    print (vars(META))

test_metadata()
