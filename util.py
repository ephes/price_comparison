import numpy as np


class Ean:
    _weights = (3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3)

    def check_ean(self, ean):
        """ Checks if the given EAN is valid, i.e. well-formed and up to
            the checksum test.
        """
        try:
            ean = str(int(str(ean).replace(" ","")))
        except ValueError:
            return False
        if len(ean) < 8:
            return False
        try:
            ean_digits = [int(d) for d in ean[:-1]]
        except ValueError:
            return False
        if len(ean_digits) > len(self._weights):
            return False
        offset = len(self._weights) - len(ean_digits)
        checksum = sum([self._weights[offset+i] * ean_digits[i] \
                         for i in range(0, len(ean_digits)) ])
        next_ten = int(checksum) / 10
        if int(checksum) % 10: next_ten += 1
        next_ten *= 10
        check_digit = next_ten - checksum
        return check_digit == int(ean[-1])

    def norm_or_nan(self, ean):
        try:
            norm_ean = str(int(float(str(ean).replace(" ",""))))
        except ValueError:
            return np.nan
        if not self.check_ean(norm_ean):
            return np.nan
        return norm_ean
