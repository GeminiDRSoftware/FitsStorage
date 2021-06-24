from sqlalchemy import Column, ForeignKey
from sqlalchemy import BigInteger, Integer, Text, Boolean, DateTime, String
from sqlalchemy.orm import relationship

import os
import datetime
import bz2

from fits_storage.orm.provenance import Provenance, ProvenanceHistory
from ..utils.hashes import md5sum, md5sum_size_bz2

from gemini_obs_db import Base
from gemini_obs_db.file import File
from gemini_obs_db.preview import Preview

from ..fits_storage_config import storage_root, z_staging_area

from io import StringIO


# ------------------------------------------------------------------------------


# TODO not sure how much if any of this would really be needed for us to get ADS
# support.  See the `Publication` type that already exists with a simple structure.
# That one is already integrated with the library.


class Bibliography(Base):
    """
    This is the ORM class for the bibliography table. A bibliography is the top
    level information that we share with the ADS.  This provides a way to map from
    published papers to the data we host in the Archive.
    """
    __tablename__ = 'bibliography'

    id = Column(Integer, primary_key=True)

    bib_code = Column(String(length=20))
    review_authors = Column(Text)
    book_authors = Column(Text)
    journal_name = Column(Text)
    journal_volume = Column(Text)
    publication_date = Column(DateTime(timezone=True), index=True)
    first_page_of_article = Column(Integer)
    last_page_of_article = Column(Integer)
    title = Column(Text)
    object_name = Column(Text)
    subject_category = Column(Text)
    origin_of_the_article_in_the_ads = Column(Text)
    score_from_the_ads_query = Column(Text)
    abstract_copyright = Column(Text)
    url_for_electronic_document = Column(Text)
    electronic_data_table = Column(Text)
    links_to_other_information = Column(Text)
    keywords = Column(Text)
    language = Column(Text)
    comment = Column(Text)
    database = Column(Text)
    doi = Column(Text)
    abstract = Column(Text)
    references = Column(Text)

    references = relationship("BibliographyReference")
    authors = relationship("BibliographyAuthor")

    def __repr__(self):
        return "<Bibliography('%s', '%s', '%s', '%s')>" % (self.id, self.bib_code, self.title)

    def ads_tagged(self):
        retval = StringIO.StringIO()

        retval.write("%%R %s\n" % (self.bib_code))
        
        retval.write("%%A ")
        first = True
        for author in self.authors:
            if not first:
                retval.write(", ")
            first = False
            retval.write("%s, %s" % (author.last_name, author.first_name))
            if author.email:
                retval.write(" <email>%s</email>" % author.email)
            retval.write(" %s %s" % (author.institution, author.location))

        retval.write("%%a %s" % (self.review_authors))

        retval.write("%%b %s" % (self.book_authors))

        if self.authors:
            retval.write("%%F ")
            idx = 'A'
            for author in self.authors:
                retval.write("%%A%s(%s, %s)\n" % (idx, author.institution, author.location))
                idx += 1
        
        retval.write("%%J %s" % (self.journal_name))

        retval.write("%%V %s" % (self.journal_volume))

        retval.write("%%D %s" % (self.publication_date))

        retval.write("%%P %d" % (self.first_page_of_article))

        retval.write("%%L %d" % (self.last_page_of_article))

        retval.write("%%T %s" % (self.title))

        retval.write("%%t %s" % (self.original_language_title))

        retval.write("%%C %s" % (self.abstract_copyright))

        retval.write("%%O %s" % (self.object_name))

        retval.write("%%Q %s" % (self.subject_category))

        retval.write("%%G %s" % (self.origin_of_the_article_in_the_ads))

        retval.write("%%S %s" % (self.score_from_the_ads_query))

        retval.write("%%E %s" % (self.electronic_data_table))

        retval.write("%%I %s" % (self.links_to_other_information))

        retval.write("%%U %s" % (self.url_for_electronic_document))

        retval.write("%%K %s" % (self.keywords))

        retval.write("%%M %s" % (self.language))

        retval.write("%%X %s" % (self.comment))

        retval.write("%%W %s" % (self.database))

        retval.write("%%Y %s" % (self.doi))

        retval.write("%%B %s" % (self.abstract))

        retval.write("%%Z %s" % (self.review_authors))

        out = retval.output()
        retval.close()

        return out


class BibliographyReference(Base):
    """
    This is the ORM class for tracking the references related to a bib record.
    """
    __tablename__ = "biblography_reference"

    id = Column(Integer, primary_key=True)

    reference = Column(Text)
    first_page = Column(Integer)
    last_page = Column(Integer)

    bibliography_id = Column(Integer, ForeignKey('bibliography.id'))

    def __repr__(self):
        return "<BibliographyReference(%s)>" % (self.reference)


class BibliographyAuthor(Base):
    """
    This is the ORM class for tracking the authors in a bib record.
    """
    __tablename__ = "bibliography_author"

    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    email = Column(Text)
    institution = Column(Text)
    location = Column(Text)

    bibliography_id = Column(Integer, ForeignKey('bibliography.id'))

    def __repr__(self):
        return "<BibliographyAuthor(%s,%s,%s)>" % (self.last_name, self.first_name, self.institution)

