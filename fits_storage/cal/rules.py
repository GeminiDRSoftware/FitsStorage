import yaml
from datetime import timedelta

from gempy.utils import logutils

log = logutils.get_logger(__name__)


class Rule:
    def __init__(self, prefix=None):
        if prefix:
            self.prefix = f"{prefix}, "
        else:
            self.prefix = ""

    def check(self, cal_obj, descriptors, processed, cal_header,cal_instrument=None):
        return "Check not implemented"

    def failure_message(self, msg):
        msg = f"{self.prefix}{msg}"
        if msg:
            msg = msg[0:1].upper() + msg[1:]
        return f"{self.prefix}{msg}"

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


class AndRule(Rule):
    def __init__(self, *rules, prefix=None):
        super().__init__(prefix=prefix)
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
    def __init__(self, descriptor, prefix=None):
        super().__init__(prefix=prefix)
        self.descriptor = descriptor

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        if not self.descriptor in descriptors.keys():
            return self.failure_message("Input data missing descriptor %s" % self.descriptor)
        if not descriptors[self.descriptor] == self._get_cal_descriptor(self.descriptor, cal_header, cal_instrument):
            return self.failure_message("Field %s did not match as required between file and calibration")
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
    def __init__(self, descriptor, prefix=None):
        super().__init__(prefix=prefix)
        self.descriptor = descriptor

    def check(self, cal_obj, descriptors, processed, cal_header, cal_instrument=None):
        calval = self._get_cal_descriptor(self.descriptor, cal_header, cal_instrument)
        if calval is None or not calval.contains(descriptors[self.descriptor]):
            return self.failure_message("Calibration %s must contain target value of %s, but it does not or is None" %
                                        (self.descriptor, descriptors[self.descriptor]))


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


def _match(rule_data):
    if isinstance(rule_data, list):
        ar = AndRule()
        for d in rule_data:
            ar.add(MatchRule(d))
        return ar
    if isinstance(rule_data, str):
        return MatchRule(rule_data)
    raise ValueError("Invalid data type for `match` rule, expecting str or list")


def _if_in(rule_data):
    return IfInRule(rule_data['descriptor'], rule_data['values'],
                    None if 'in' not in rule_data else parse_rules(rule_data['in']),
                    None if 'not_in' not in rule_data else parse_rules(rule_data['not_in']))


def _if_processed(rule_data):
    return IfProcessedRule(rule_data)


def _if_not_none(rule_data):
    return IfNotNoneRule(rule_data['descriptor'],
                         None if 'not_none' not in rule_data else parse_rules(rule_data['not_none']))


def _cal_contains_rule(rule_data):
    return CalContainsRule(rule_data)


rule_factory = {
    'raw_or_processed': lambda rule_data: RawOrProcessedRule(rule_data),
    'match': _match,
    'max_interval': lambda rule_data: MaxIntervalRule(rule_data),
    'if_in': _if_in,
    'if_processed': _if_processed,
    'if_not_none': _if_not_none,
    'cal_contains': _cal_contains_rule,
}


def parse_rule(dict):
    rules = list()
    rule_name = next(iter(dict))
    rule_data = dict[rule_name]
    return rule_factory[rule_name](rule_data)


def parse_rules(rules_list):
    # list of single-element dicts
    rules = list()
    for rule_dict in rules_list:
        rule = parse_rule(rule_dict)
        if rule is not None:
            rules.append(rule)
    if len(rules) > 1:
        ar = AndRule()
        for rule in rules:
            ar.add(rule)
        return ar
    elif len(rules)==1:
        return rules[0]
    else:
        return None


def parse_yaml(filename, cal_type):
    rules = list()

    a_yaml_file = open(filename)
    parsed_yaml_file = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    # log.info("Yaml Parsed Rules:\n%s" % parsed_yaml_file)
    cal_type_dict = parsed_yaml_file[cal_type]
    for rule_dict in cal_type_dict['rules']:
        rule = parse_rule(rule_dict)
        if rule is not None:
            rules.append(rule)
    return rules
