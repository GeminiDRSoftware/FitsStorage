from datetime import timedelta


class Rule:
    def __init__(self):
        pass

    def check(self, cal_obj, descriptors, processed, cal_header,cal_instrument=None):
        return "Check not implemented"

    def _get_cal_descriptor(self, descriptor, cal_header, cal_instrument):
        if descriptor in cal_obj.instrDescriptors:
            if cal_instrument is None:
                return None
            if not hasattr(cal_instrument, descriptor):
                return None
            return getattr(cal_instrument, descriptor)
        else:
            if not hasattr(cal_header, descriptor):
                return None
            return getattr(cal_header, descriptor)


class AndRule:
    def __init__(self, *rules):
        self.rules = list()
        for rule in rules:
            self.add(rule)

    def add(self, rule):
        self.rules.append(rule)

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        for rule in self.rules:
            result = rule.check(descriptors, processed, cal_header, cal_instrument)
            if result is not None:
                return result


class MatchRule(Rule):
    def __init__(self, descriptor):
        self.descriptor = descriptor

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        if not self.descriptor in descriptors.keys():
            return "Input data missing descriptor %s" % self.descriptor
        if not descriptors[self.descriptor] == self._get_cal_descriptor(self.descriptor, cal_header, cal_instrument):
            return "Field %s did not match as required between file and calibration"
        return None

    def buildquery(self, query, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        return query.match_descriptor(self.descriptor)

class IfInRule(Rule):
    def __init__(self, descriptor, values, rule, else_rule=None):
        self.descriptor = descriptor
        self.values = values
        self.rule = rule
        self.else_rule = else_rule

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        if not self.descriptor in descriptors.keys():
            return None
        if descriptors[self.descriptor] in self.values:
            check = self.rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
            if check:
                return "Since %s is in values %s: %s" % (self.descriptor, self.values, check)
        else:
            if self.else_rule:
                check = self.else_rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
                if check:
                    return "Since %s is not in values %s: %s" % (self.descriptor, self.values, check)
        return None

    def updatequery(self):
        pass


class IfNotNoneRule(Rule):
    def __init__(self, descriptor, rule):
        self.descriptor = descriptor
        self.rule = rule

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        if self.descriptors[self.descriptor] is not None:
            chk = self.rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
            if chk is not None:
                return "Since %s is not None: %s" % (self.descriptor, chk)
        return None


class IfProcessedRule(Rule):
    def __init__(self, rule, else_rule=None):
        self.rule = rule
        self.else_rule = else_rule

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument):
        if processed:
            if self.rule is not None:
                chk = self.rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
                if chk:
                    return "Processed calibration and %s" % chk
        else:
            if self.else_rule is not None:
                chk = self.else_rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
                if chk:
                    return "Not processed calibration and %s" % chk
        return None


class IfEqualsRule(Rule):
    def __init__(self, descriptor, value, rule, else_rule=None):
        self.descriptor = descriptor
        self.value = value
        self.rule = rule
        self.else_rule = else_rule

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument):
        val = self._get_cal_descriptor(self.descriptor, cal_header, cal_instrument)
        if self.value == val:
            chk = self.rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
            if chk:
                return "Calibration descriptor %s matched value of %s and %s" % (self.descriptor, self.value, chk)
        elif self.else_rule:
            chk = self.else_rule.check(cal_obj, descriptors, processed, cal_header, cal_instrument)
            if chk:
                return "Calibration descriptor %s didn't match value of %s and %s" % \
                       (self.descriptor, self.value, chk)
        return None


class CalContainsRule(Rule):
    def __init__(self, descriptor):
        self.descriptor = descriptor

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        calval = self._get_cal_descriptor(self.descriptor, cal_header, cal_instrument)
        if calval is None or not calval.contains(descriptors[self.descriptor]):
            return "Calibration %s must contain target value of %s, but it does not or is None" % \
                   (self.descriptor, descriptors[self.descriptor])


class MaxIntervalRule(Rule):
    def __init__(self, interval):
        self.interval = interval

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        target_ut = descriptors['ut_datetime']
        cal_ut = cal_header.ut_datetime
        if abs(target_ut - cal_ut) > timedelta(days=self.interval):
            return "Calibration outside the required interval of %d days" % self.interval
        return None


class RawOrProcessedRule(Rule):
    def __init__(self, typ):
        self.typ = typ

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        if processed:
            if cal_header.reduction != 'PROCESSED_' + self.typ:
                return "For processed data, calibration must have a reduction type of PROCESSED_%s" % self.typ
        else:
            if cal_header.reduction != 'RAW':
                return "For non-processed data, calibration must have reduction type of RAW"
            if cal_header.observation_type != self.typ:
                return "Observation Type of calibration is not %s" % self.typ
        return None

    def updatequery(self, cal_obj, descriptors, processed, query):
        if processed:
            return query.reduction('PROCESSED_' + self.typ)
        else:
            return query.reduction('RAW').observation_type(self.typ)