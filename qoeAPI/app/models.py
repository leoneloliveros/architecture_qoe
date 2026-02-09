from . import db

class Dg_maximo_ci(db.Model):
    __tablename__ = 'CI'
    __table_args__= {'schema': 'MAXIMO'}
    cinum = db.Column(db.String(150), primary_key=True)
    classstructureid = db.Column(db.String(100))
    cilocation = db.Column(db.String(100))
    ciname = db.Column(db.Text)
    status = db.Column(db.String(20))
    description = db.Column(db.String(1024))


class Dg_maximo_workorder(db.Model):
    __tablename__ = 'WORKORDER'
    __table_args__= {'schema': 'MAXIMO'}
    wonum = db.Column(db.String(150), primary_key=True)
    status = db.Column(db.String(20))
    worktype = db.Column(db.String(20))
    description = db.Column(db.String(300))
    location = db.Column(db.String(50))
    origrecordid = db.Column(db.String(30))

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    

class Dg_maximo_woactivity(db.Model):
    __tablename__ = 'WOACTIVITY'
    __table_args__= {'schema': 'MAXIMO'}
    wonum = db.Column(db.String(150), primary_key=True)
    origrecordid = db.Column(db.String(20))
    description = db.Column(db.String(300))
    ownergroup = db.Column(db.String(50))
    status = db.Column(db.String(20))


    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class Dg_maximo_worklog(db.Model):
    __tablename__ = 'WORKLOG'
    __table_args__= {'schema': 'MAXIMO'}
    rowstamp = db.Column(db.String(50), primary_key=True)
    recordkey = db.Column(db.String(20))
    description = db.Column(db.String(300))

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}