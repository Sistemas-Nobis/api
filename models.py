from pydantic import BaseModel, field_validator, ValidationError, FieldValidationInfo, model_validator, Field, ValidationInfo
import json
from pydantic_core import PydanticCustomError
import os
from typing import Optional


with open("entidades.json", "r", encoding="utf-8") as f:
    ENTIDADES = json.load(f)


# Modelo para los datos que se recibirán
class MovfPago(BaseModel):
    #age_id: int
    fpago_id: int
    entfin_id: int
    #movfp_desde: int
    #movfp_hasta: int
    cbu: str
    numero: str
    vencimiento: str
    #movfp_id : int
    nombre_entidad: Optional[str] = Field(default=None)

    # Validador para el campo cbu
    @field_validator("fpago_id")
    def validate_fpago(cls, value):
        if not value in (1,2,3):
            raise ValueError("La forma de pago ingresada NO es valida.")
        return value
    
    # Validador para el campo movfp_desde
    #@field_validator("movfp_desde")
    #def validate_movfp_desde(cls, value):
    #    if not (100000 <= value <= 999999):
    #        raise ValueError("movfp_desde debe tener 6 dígitos en formato yyyymm")
    #    year = value // 100
    #    month = value % 100
    #    if not (1 <= month <= 12):
    #        raise ValueError("movfp_desde debe tener un mes válido entre 01 y 12")
    #    return value

    # Validador para el campo movfp_hasta
    #@field_validator("movfp_hasta")
    #def validate_movfp_hasta(cls, value):
    #    if not (100000 <= value <= 999999):
    #        raise ValueError("movfp_hasta debe tener 6 dígitos en formato yyyymm")
    #    year = value // 100
    #    month = value % 100
    #    if not (1 <= month <= 12):
    #        raise ValueError("movfp_hasta debe tener un mes válido entre 01 y 12")
    #    return value


    @field_validator("cbu") # SOLO DEBITO
    def validate_cbu(cls, value: str, info: FieldValidationInfo) -> str:
        fpago_id = info.data.get("fpago_id")
        
        # Si fpago_id == 1, el campo CBU debe estar vacío
        if fpago_id == 1 or fpago_id == 2:
            if value != "":
                raise ValueError("El campo 'cbu' debe estar vacío cuando 'fpago_id' es 1 o 2.")
            return value
        
        # Si fpago_id != 1, el CBU debe ser válido y no puede estar vacío
        if fpago_id == 3 and not value.strip():
            raise ValueError("El campo 'cbu' es requerido cuando 'fpago_id' es 3.")
        
        cbu_str = value.strip()
        
        # Cargar entidades desde JSON
        try:
            # Buscar el archivo JSON en el mismo directorio que este archivo
            current_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(current_dir, "entidades.json")
            
            with open(json_path, 'r', encoding='utf-8') as file:
                entidades_list = json.load(file)
            
            # Convertir lista a diccionario para búsqueda rápida
            ENTIDADES = {item["codigo"]: item["entidad"] for item in entidades_list}
            
        except FileNotFoundError:
            raise ValueError("No se encontró el archivo 'entidades_bancarias.json' en el directorio del modelo")
        except json.JSONDecodeError:
            raise ValueError("Error al decodificar el archivo JSON de entidades bancarias")
        except KeyError as e:
            raise ValueError(f"Estructura incorrecta en el archivo JSON de entidades: falta la clave {e}")
        
        # 1. Validar longitud y que sea numérico
        if len(cbu_str) != 22 or not cbu_str.isdigit():
            raise ValueError("El CBU debe contener exactamente 22 dígitos numéricos")
        
        # 2. Extraer código de entidad y validar que esté registrada
        codigo_entidad = cbu_str[:3]
        if codigo_entidad not in ENTIDADES:
            raise ValueError("El CBU ingresado no corresponde a una entidad bancaria registrada (posible CVU)")
        
        # 3. Validar primer dígito verificador (posición 8)
        pesos1 = [7, 1, 3, 9, 7, 1, 3]
        suma1 = sum(int(cbu_str[i]) * pesos1[i] for i in range(7))
        verificador1 = (10 - (suma1 % 10)) % 10
        if verificador1 != int(cbu_str[7]):
            raise ValueError("El primer dígito verificador del CBU es incorrecto")
        
        # 4. Validar segundo dígito verificador (posición 22)
        pesos2 = [3, 9, 7, 1, 3, 9, 7, 1, 3, 9, 7, 1, 3]
        suma2 = sum(int(cbu_str[i + 8]) * pesos2[i] for i in range(13))
        verificador2 = (10 - (suma2 % 10)) % 10
        if verificador2 != int(cbu_str[21]):
            raise ValueError("El segundo dígito verificador del CBU es incorrecto")
        
        # 5. Si llegamos aquí, el CBU es válido
        nombre_entidad = ENTIDADES[codigo_entidad]
        #print(f"Tipo: CBU - Entidad: {nombre_entidad}")
        
        return value
    
    @model_validator(mode="after")
    def set_nombre_entidad(self):
        cbu_str = self.cbu.strip()
        if len(cbu_str) != 22:
            return self  # Validación previa ya lo bloqueó

        codigo_entidad = cbu_str[:3]
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(current_dir, "entidades.json")
            with open(json_path, 'r', encoding='utf-8') as file:
                entidades_list = json.load(file)
            ENTIDADES = {item["codigo"]: item["entidad"] for item in entidades_list}
        except Exception:
            ENTIDADES = {}

        if codigo_entidad in ENTIDADES:
            self.nombre_entidad = ENTIDADES[codigo_entidad]

        return self

    
    # Validador para el campo numero (tarjeta de crédito)
    @field_validator("numero")
    def validate_numero(cls, value, info: FieldValidationInfo):
        fpago_id = info.data.get("fpago_id")  # Accede a otros campos mediante info.data
        entfin_id = info.data.get("entfin_id")

        if fpago_id == 1 or fpago_id == 3:
            if value != "":
                raise ValueError("El campo numero debe estar vacío cuando 'fpago_id' es 1 o 3.")
        if fpago_id == 2 and not value.isdigit():
            raise ValueError("El número de tarjeta debe contener solo dígitos.")
        else:
            if entfin_id == 6:
                return value
            
            else:

                # Implementar el algoritmo de Luhn
                digitos = [int(d) for d in value][::-1]  # Invertir el número
                suma = 0
                for i, digito in enumerate(digitos):
                    if i % 2 == 1:  # Duplicar dígitos en posiciones impares
                        doble = digito * 2
                        suma += doble if doble < 10 else doble - 9
                    else:
                        suma += digito

                if suma % 10 != 0:
                    raise ValueError("El número de tarjeta no es válido.")
        
        return value
    
    # Validador para la relación entre fpago_id, numero y entfin_id
    @field_validator("entfin_id")
    def validate_entfin_id(cls, value, info: FieldValidationInfo):
        
        fpago_id = info.data.get("fpago_id")
        numero = info.data.get("numero","")
        
        if fpago_id == 1:
            if value != 0:
                raise ValueError("El campo 'entfin_id' debe ser 0 cuando 'fpago_id' es 1.")
            if numero not in (None, ""):
                raise ValueError("El campo 'numero' debe estar vacío cuando 'fpago_id' es 1.")
        elif fpago_id == 2:
            if value not in (3,6,7,8):
                raise ValueError("Entidad financiera invalida para esta forma de pago.")
        elif fpago_id == 3:
            if value not in (71,100):
                raise ValueError("Entidad financiera invalida para esta forma de pago.")
        
        return value