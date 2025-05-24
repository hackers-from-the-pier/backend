from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
from sqlalchemy import DateTime, Table, Column, MetaData, ForeignKey, UUID, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, Text, CHAR, Boolean, Integer, Float, ARRAY, JSON
from sqlalchemy import Enum as SQLAlchemyEnum

# Базовый класс для всех моделей
class Base(DeclarativeBase):
    pass

# Метаданные для SQLAlchemy
table_metadata = Base.metadata

class User(Base): # пользователь
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True, autoincrement=True) # id пользователя
    role: Mapped[str] = mapped_column(Text) # роль пользователя
    
    # Отношения
    reports: Mapped[List["Report"]] = relationship("Report", back_populates="staff") # отчеты пользователя

class Report(Base): # отчет
    __tablename__ = "reports"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, index=True, autoincrement=True)# id отчета
    staff_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=True)# id пользователя
    is_ready: Mapped[bool] = mapped_column(Boolean, default=False)# готов ли отчет
    
    # Отношения
    staff: Mapped["User"] = relationship("User", back_populates="reports") # пользователь, который создал отчет
    
class File(Base): # загруженный файл
    __tablename__ = "files"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True) # id файла
    is_parsed: Mapped[bool] = mapped_column(Boolean, default=False) # был ли ранее обработан
    report_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("reports.id")) # id отчета
    s3_url: Mapped[str] = mapped_column(Text) # url файла в s3

class Client(Base): # клиент
    __tablename__ = "clients"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True) # id клиента
    name: Mapped[str] = mapped_column(Text, nullable=True) # имя клиента     
    email: Mapped[str] = mapped_column(Text, nullable=True) # email клиента   
    phone: Mapped[str] = mapped_column(Text, nullable=True) # телефон клиента 
    address: Mapped[str] = mapped_column(Text, nullable=True) # адрес клиента
    is_commercial: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False) # коммерческий клиент
    home_type: Mapped[str] = mapped_column(Text, nullable=True) # тип дома
    home_area: Mapped[float] = mapped_column(Float, nullable=True) # площадь дома м2
    season_index: Mapped[float] = mapped_column(Float, nullable=True) # сезонный индекс
    people_count: Mapped[int] = mapped_column(Integer, nullable=True) # количество жильцов
    rooms_count: Mapped[int] = mapped_column(Integer, nullable=True) # количество комнат
    
    frod_state: Mapped[str] = mapped_column(Text, nullable=True) # Статус Фрода
    frod_procentage: Mapped[float] = mapped_column(Float, nullable=True) # Процент фрода
    frod_yandex: Mapped[str] = mapped_column(Text, nullable=True) # Яндекс ссылка на объект
    frod_avito: Mapped[str] = mapped_column(Text, nullable=True) # Авито ссылка на объект
    frod_2gis: Mapped[str] = mapped_column(Text, nullable=True) # 2GIS ссылка на объект
    