from pydantic import BaseModel, field_validator, ValidationError, FieldValidationInfo

# Modelo para los datos que se recibirán
class MovfPago(BaseModel):
    age_id: int
    fpago_id: int
    entfin_id: int
    movfp_desde: int
    movfp_hasta: int
    cbu: str
    numero: str
    vencimiento: str
    movfp_id : int

    # Validador para el campo cbu
    @field_validator("fpago_id")
    def validate_fpago(cls, value):
        if not value in (1,2,3):
            raise ValueError("La forma de pago ingresada NO es valida.")
        return value
    
    # Validador para el campo movfp_desde
    @field_validator("movfp_desde")
    def validate_movfp_desde(cls, value):
        if not (100000 <= value <= 999999):
            raise ValueError("movfp_desde debe tener 6 dígitos en formato yyyymm")
        year = value // 100
        month = value % 100
        if not (1 <= month <= 12):
            raise ValueError("movfp_desde debe tener un mes válido entre 01 y 12")
        return value

    # Validador para el campo movfp_hasta
    @field_validator("movfp_hasta")
    def validate_movfp_hasta(cls, value):
        if not (100000 <= value <= 999999):
            raise ValueError("movfp_hasta debe tener 6 dígitos en formato yyyymm")
        year = value // 100
        month = value % 100
        if not (1 <= month <= 12):
            raise ValueError("movfp_hasta debe tener un mes válido entre 01 y 12")
        return value
    
    # Validador para el campo cbu
    @field_validator("cbu")
    def validate_cbu(cls, value, info: FieldValidationInfo):
        fpago_id = info.data.get("fpago_id")  # Accede a otros campos mediante info.data
        if fpago_id == 1:
            if value != "":
                raise ValueError("El campo 'cbu' debe estar vacío cuando 'fpago_id' es 1.")
            else:
                return value
        else:
        
            cbu_str = value

            # Verificar que el CBU tenga 22 dígitos
            if not cbu_str.isdigit() or len(cbu_str) != 22:
                raise ValueError("El CBU debe contener exactamente 22 dígitos")

            # Validar el primer dígito verificador (posición 8)
            pesos1 = [7, 1, 3, 9, 7, 1, 3]
            suma1 = sum(int(cbu_str[i]) * pesos1[i] for i in range(7))
            verificador1 = (10 - (suma1 % 10)) % 10
            if verificador1 != int(cbu_str[7]):
                raise ValueError("El primer dígito verificador del CBU es incorrecto")

            # Validar el segundo dígito verificador (posición 22)
            pesos2 = [3, 9, 7, 1, 3, 9, 7, 1, 3, 9, 7, 1, 3]
            suma2 = sum(int(cbu_str[i + 8]) * pesos2[i] for i in range(13))
            verificador2 = (10 - (suma2 % 10)) % 10
            if verificador2 != int(cbu_str[21]):
                raise ValueError("El segundo dígito verificador del CBU es incorrecto")

        return value
    
    # Validador para el campo numero (tarjeta de débito/crédito)
    @field_validator("numero")
    def validate_numero(cls, value, info: FieldValidationInfo):
        fpago_id = info.data.get("fpago_id")  # Accede a otros campos mediante info.data
        entfin_id = info.data.get("entfin_id")

        if fpago_id == 1 and value != "":
            raise ValueError("El campo 'numero debe estar vacío cuando 'fpago_id' es 1.")
        if fpago_id in (2, 3) and not value.isdigit():
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
        else:
            if value not in (3,6,7,8,71,100):
                raise ValueError("Entidad financiera invalida.")
        
        return value