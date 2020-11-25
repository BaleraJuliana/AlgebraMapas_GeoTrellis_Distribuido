package tutorial

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._
import scala.collection.mutable.ListBuffer
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._
import geotrellis.spark.io.index.zcurve



import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File

object testespark {

	def logica_ComputeVegetationFactor(vegetation_class: Double) : Double = {
		
		    if (vegetation_class == 0 || vegetation_class == 13 || vegetation_class==15 || vegetation_class == 16) {
			return 0
		    }  
		    if (vegetation_class == 2 || vegetation_class==11) {
			return 1.5
		    }
		    if (vegetation_class==4) {
			return 1.72
		    }
		    if (vegetation_class==1 || vegetation_class==3 || vegetation_class==5) {
			return 2
		    }
		    if (vegetation_class==6 || vegetation_class==8) {
			return 2.4
		    }
		    if (vegetation_class==7 || vegetation_class==9) {
			return 3
		    }	
		    if (vegetation_class==12 || vegetation_class==14) {
			return 4
		    }
		    if (vegetation_class==10) {
			return 6
		    }
		    return 0
	  }



	def logica_CapByVegetation(igbp_class: Double, pse: Double): Double = {

	//	val ibge_class = ConvertFromIGBP_To_IBGE(igbp_class)
	
		if(igbp_class == 1){
			if(pse<30){
				return pse	
			} else {
				return 30
			}	
		}
		if(igbp_class == 2){
			if(pse<45){
				return pse	
			} else {
				return 45
			}	
		}
		if(igbp_class == 3){
			if(pse<60){
				return pse	
			} else {
				return 60
			}	
		}
		if(igbp_class == 4){
			if(pse<75){
				return pse	
			} else {
				return 75
			}	
		}
		if(igbp_class == 5){
			if(pse<90){
				return pse	
			} else {
				return 90
			}	
		}
		if(igbp_class == 6){
			if(pse<105){
				return pse	
			} else {
				return 105
			}	
		}
		if(igbp_class == 7){
			if(pse<120){
				return pse	
			} else {
				return 120
			}	
		}
		return NODATA
	}
	
	/**
	def sumPAndCalculateFP(_tile: Tile, const: Double): Tile = {
		var colunas = _tile.cols
		val linhas = _tile.rows
		val cellType = _tile.cellType
				
		var i = 0
		var j = 0
		val tile = ArrayTile.empty(cellType, colunas, linhas)

		for(i<-0 to linhas){
			for(j<-0 to colunas){
				var valor_atualizado = exp(_tile.tile.get(i,j)*const)
				tile.setDouble(i, j, valor_atualizado)
			}
		}
		return tile
	}	
	**/


	def multiplicar(tile1: Tile, tile2: Tile): Tile = {	
		
		val tile = tile1*tile2
		return tile
	}
	
	
	def ComputeVegetationFactor(vegetation_class: Tile): Tile = {

		var retorno = 0
		var colunas = vegetation_class.cols
		var linhas = vegetation_class.rows
		var cellType = vegetation_class.cellType

		val tile = ArrayTile.empty(cellType, colunas, linhas)
		
		for(i<-0 to linhas){
			for(j<-0 to colunas){
				 var valor_atualizado = logica_ComputeVegetationFactor(vegetation_class.tile.get(i,j));			
				tile.setDouble(i, j, valor_atualizado)
			}
		}
		return tile 
	}

	def CapByVegetation(igbp_class: Tile, pse: Tile): Tile = {

		var retorno = 0
		var colunas = igbp_class.cols
		var linhas = igbp_class.rows
		var cellType = igbp_class.cellType

		val tile = ArrayTile.empty(cellType, colunas, linhas)
		
		for(i<-0 to linhas){
			for(j<-0 to colunas){
				var valor_atualizado = logica_CapByVegetation(igbp_class.tile.get(i,j), pse.tile.get(i,j));			
				tile.setDouble(i, j, valor_atualizado)
			}
		}
		return tile 
	}

	def calculationRB(tile: Tile, const: Double): Tile = 
	{
		return tile.tile*const
	
	}
	
	// interface do programa

	var nc = ""
	var anos_validos = Set("2016", "2017")
	
	println("")
	println("")
	println(Console.GREEN+"+---------------------------------------------------------------------------------------+")
	println(Console.GREEN+"|"+Console.GREEN+"\tOoi ! esse script gera o mapa de risco de fogo para um determinado ano :)\t"+Console.GREEN+"|")
	//println("|\t									     \t|")
	println("+---------------------------------------------------------------------------------------+")
	println(Console.GREEN+"|"+Console.GREEN+"\tPodemos realizar os cáculos para os seguintes anos:  		     	\t"+Console.GREEN+"|")
	println(Console.GREEN+"|\t "+Console.GREEN+">2016									\t"+Console.GREEN+ "|")									
	println(Console.GREEN+"|\t "+Console.GREEN+">2017									\t"+Console.GREEN+"|")	
	println("+---------------------------------------------------------------------------------------+")
	println("")
	println("")
	var ano = readLine(Console.WHITE+"Digite o ano: "+Console.WHITE+"")
	
	while(!(anos_validos contains ano))
	{
		println(" ")
		println(Console.RED + s">>>>>Vc errou :( O ano de ${ano} não faz parte da nossa base de dados !<<<<<")
		ano = readLine(Console.WHITE+"Digite o ano, novamente: "+Console.WHITE+"")
			
	}	

	if(ano=="2016")
	{
		nc = ".nc"
	}	
	
	println(Console.WHITE)

	
	def main(args: Array[String]): Unit = {
		
		
		// iniciando o ambiente spark
		//val conf = new SparkConf().setMaster("local").setAppName("Testando o Spark")
		//val sc = new SparkContext(conf)
		
		 val conf =
	     		new SparkConf()
	        		.setMaster("local[*]")
	        		.setAppName("Spark Tiler")
				.set("spark.kryoserializer.buffer.max.mb", "512")
	        		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	        		.set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

	    	val sc = new SparkContext(conf)
	   
	    
	    	try {
	      		run(sc)
	      		// Pause to wait to close the spark context,
	      		// so that you can check out the UI at http://localhost:4040
	      		println("Hit enter to exit.")
	     		StdIn.readLine()
	    	} finally {
	      		sc.stop()
		}	
	}	
	
	def run(implicit sc: SparkContext) = {
		
		val lista_RDDs = ListBuffer[RDD[(ProjectedExtent, MultibandTile)]]()
		val lista_RDDs_prontos = ListBuffer[(RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]])]()
		
		//carregando os dados do diretório...
		println(Console.CYAN+"Lendo os dados..."+Console.WHITE)	
		val p1: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07151200${nc}.tif")

		val p2: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07141200${nc}.tif")

		val p3: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07131200${nc}.tif")
		
		val p4: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07121200${nc}.tif")

		val p5: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07111200${nc}.tif")

		val p10_6: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07101200${nc}.tif")

		val p10_7: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07091200${nc}.tif")

		val p10_8: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07081200${nc}.tif")

		val p10_9: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07071200${nc}.tif")

		val p10_10: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07061200${nc}.tif")
		val p15_11: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07051200${nc}.tif")
		val p15_12: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06041200${nc}.tif")
		val p15_13: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07031200${nc}.tif")
		val p15_14: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07021200${nc}.tif")
		val p15_15: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07011200${nc}.tif")
		val p30_16: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06301200${nc}.tif")
		val p30_17: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06291200${nc}.tif")

		val p30_18: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06281200${nc}.tif")

		val p30_19: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06271200${nc}.tif")

		val p30_20: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06261200${nc}.tif")

		val p30_21: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06251200${nc}.tif")

		val p30_22: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06241200${nc}.tif")

		val p30_23: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06231200${nc}.tif")

		val p30_24: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06221200${nc}.tif")

		val p30_25: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06211200${nc}.tif")

		val p30_26: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06191200${nc}.tif")

		val p30_27: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06181200${nc}.tif")

		val p30_28: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06171200${nc}.tif")

		val p30_29: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06161200${nc}.tif")
		val p30_30: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06151200${nc}.tif")
		val p60_31: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06141200${nc}.tif")
		val p60_32: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06131200${nc}.tif")

		val p60_33: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06121200${nc}.tif")

		val p60_34: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06111200${nc}.tif")

		val p60_35: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06101200${nc}.tif")

		val p60_36: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06091200${nc}.tif")

		val p60_37: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06081200${nc}.tif")

		val p60_38: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06071200${nc}.tif")

		val p60_39: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06061200${nc}.tif")

		val p60_40: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06051200${nc}.tif")

		val p60_41: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06041200${nc}.tif")

		val p60_42: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06031200${nc}.tif")

		val p60_43: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06021200${nc}.tif")

		val p60_44: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}06011200${nc}.tif")

		val p60_45: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05311200${nc}.tif")

		val p60_46: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05301200${nc}.tif")

		val p60_47: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05291200${nc}.tif")

		val p60_48: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05281200${nc}.tif")

		val p60_49: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05271200${nc}.tif")

		val p60_50: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05261200${nc}.tif")

		val p60_51: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05251200${nc}.tif")

		val p60_52: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05241200${nc}.tif")

		val p60_53: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05231200${nc}.tif")

		val p60_54: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05221200${nc}.tif")

		val p60_55: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05211200${nc}.tif")

		val p60_56: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05201200${nc}.tif")

		val p60_57: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05191200${nc}.tif")


		val p60_58: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05181200${nc}.tif")

		val p60_59: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05171200${nc}.tif")

		val p60_60: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05161200${nc}.tif")

		val p90_61: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05151200${nc}.tif")

		val p90_62: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05141200${nc}.tif")
		val p90_63: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05131200${nc}.tif")
		val p90_64: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05121200${nc}.tif")

		val p90_65: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05111200${nc}.tif")

		val p90_66: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05101200${nc}.tif")

		val p90_67: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05091200${nc}.tif")

		val p90_68: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05081200${nc}.tif")

		val p90_69: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05071200${nc}.tif")

		val p90_70: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05061200${nc}.tif")

		val p90_71: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05051200${nc}.tif")

		val p90_72: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05041200${nc}.tif")

		val p90_73: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05031200${nc}.tif")

		val p90_74: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05021200${nc}.tif")
		val p90_75: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}05011200${nc}.tif")
		val p90_76: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04301200${nc}.tif")
		val p90_77: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04291200${nc}.tif")
		val p90_78: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04281200${nc}.tif")
		val p90_79: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04271200${nc}.tif")
		val p90_80: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04261200${nc}.tif")
		val p90_81: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04251200${nc}.tif")
		val p90_82: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04241200${nc}.tif")
		val p90_83: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04231200${nc}.tif")
		val p90_84: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04221200${nc}.tif")
		val p90_85: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04211200${nc}.tif")
		val p90_86: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04201200${nc}.tif")
		val p90_87: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04191200${nc}.tif")
		val p90_88: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04181200${nc}.tif")
		val p90_89: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04171200${nc}.tif")
		val p90_90: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04161200${nc}.tif")
		val p120_91: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04151200${nc}.tif")
		val p120_92: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04141200${nc}.tif")
		val p120_93: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04131200${nc}.tif")
		val p120_94: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04121200${nc}.tif")
		val p120_95: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04111200${nc}.tif")
		val p120_96: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04101200${nc}.tif")
		val p120_97: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04091200${nc}.tif")
		val p120_98: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04081200${nc}.tif")
		val p120_99: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04071200${nc}.tif")
		val p120_100: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04061200${nc}.tif")
		val p120_101: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04051200${nc}.tif")
		val p120_102: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04041200${nc}.tif")
		val p120_103: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04031200${nc}.tif")
		val p120_104: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04021200${nc}.tif")
		val p120_105: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}04011200${nc}.tif")
		val p120_106: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03311200${nc}.tif")
		val p120_107: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03301200${nc}.tif")
		val p120_108: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03291200${nc}.tif")
		val p120_109: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03281200${nc}.tif")
		val p120_110: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03271200${nc}.tif")
		val p120_111: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03261200${nc}.tif")
		val p120_112: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03251200${nc}.tif")
		val p120_113: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03241200${nc}.tif")
		val p120_114: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03231200${nc}.tif")
		val p120_115: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03221200${nc}.tif")
		val p120_116: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03211200${nc}.tif")
		val p120_117: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03201200${nc}.tif")
		val p120_118: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03191200${nc}.tif")
		val p120_119: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03181200${nc}.tif")
		val p120_120: RDD[(ProjectedExtent, MultibandTile)] =
		      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}03171200${nc}.tif")
		
		lista_RDDs++= List(p1)
		lista_RDDs++= List(p2)
		lista_RDDs++= List(p3)
		lista_RDDs++= List(p4)
		lista_RDDs++= List(p5)
		lista_RDDs++= List(p10_6)
		lista_RDDs++= List(p10_7)
		lista_RDDs++= List(p10_8)
		lista_RDDs++= List(p10_9)
		lista_RDDs++= List(p10_10)
		lista_RDDs++= List(p15_11)
		lista_RDDs++= List(p15_12)
		lista_RDDs++= List(p15_13)
		lista_RDDs++= List(p15_14)
		lista_RDDs++= List(p15_15)
		lista_RDDs++= List(p30_16)
		lista_RDDs++= List(p30_17)
		lista_RDDs++= List(p30_18)
		lista_RDDs++= List(p30_19)
		lista_RDDs++= List(p30_20)
		lista_RDDs++= List(p30_21)
		lista_RDDs++= List(p30_22)
		lista_RDDs++= List(p30_23)
		lista_RDDs++= List(p30_24)
		lista_RDDs++= List(p30_25)
		lista_RDDs++= List(p30_26)
		lista_RDDs++= List(p30_27)
		lista_RDDs++= List(p30_28)
		lista_RDDs++= List(p30_29)
		lista_RDDs++= List(p30_30)
		lista_RDDs++= List(p60_31)
		lista_RDDs++= List(p60_32)
		lista_RDDs++= List(p60_33)
		lista_RDDs++= List(p60_34)
		lista_RDDs++= List(p60_35)
		lista_RDDs++= List(p60_36)
		lista_RDDs++= List(p60_37)
		lista_RDDs++= List(p60_38)
		lista_RDDs++= List(p60_39)
		lista_RDDs++= List(p60_40)
		lista_RDDs++= List(p60_41)
		lista_RDDs++= List(p60_42)
		lista_RDDs++= List(p60_43)
		lista_RDDs++= List(p60_44)
		lista_RDDs++= List(p60_45)
		lista_RDDs++= List(p60_46)
		lista_RDDs++= List(p60_47)
		lista_RDDs++= List(p60_48)
		lista_RDDs++= List(p60_49)
		lista_RDDs++= List(p60_50)
		lista_RDDs++= List(p60_51)
		lista_RDDs++= List(p60_52)
		lista_RDDs++= List(p60_53)
		lista_RDDs++= List(p60_54)
		lista_RDDs++= List(p60_55)
		lista_RDDs++= List(p60_56)
		lista_RDDs++= List(p60_57)
		lista_RDDs++= List(p60_58)
		lista_RDDs++= List(p60_59)
		lista_RDDs++= List(p60_60)
		lista_RDDs++= List(p90_61)
		lista_RDDs++= List(p90_62)
		lista_RDDs++= List(p90_63)
		lista_RDDs++= List(p90_64)
		lista_RDDs++= List(p90_65)
		lista_RDDs++= List(p90_66)
		lista_RDDs++= List(p90_67)
		lista_RDDs++= List(p90_68)
		lista_RDDs++= List(p90_69)
		lista_RDDs++= List(p90_70)
		lista_RDDs++= List(p90_71)
		lista_RDDs++= List(p90_72)
		lista_RDDs++= List(p90_73)
		lista_RDDs++= List(p90_74)
		lista_RDDs++= List(p90_75)
		lista_RDDs++= List(p90_76)
		lista_RDDs++= List(p90_77)
		lista_RDDs++= List(p90_78)
		lista_RDDs++= List(p90_79)
		lista_RDDs++= List(p90_80)
		lista_RDDs++= List(p90_81)
		lista_RDDs++= List(p90_82)
		lista_RDDs++= List(p90_83)
		lista_RDDs++= List(p90_84)
		lista_RDDs++= List(p90_85)
		lista_RDDs++= List(p90_86)
		lista_RDDs++= List(p90_87)
		lista_RDDs++= List(p90_88)
		lista_RDDs++= List(p90_89)
		lista_RDDs++= List(p90_90)
		lista_RDDs++= List(p120_91)
		lista_RDDs++= List(p120_92)
		lista_RDDs++= List(p120_93)
		lista_RDDs++= List(p120_94)
		lista_RDDs++= List(p120_95)
		lista_RDDs++= List(p120_96)
		lista_RDDs++= List(p120_97)
		lista_RDDs++= List(p120_98)
		lista_RDDs++= List(p120_99)
		lista_RDDs++= List(p120_100)
		lista_RDDs++= List(p120_101)
		lista_RDDs++= List(p120_102)
		lista_RDDs++= List(p120_103)
		lista_RDDs++= List(p120_104)
		lista_RDDs++= List(p120_105)
		lista_RDDs++= List(p120_106)
		lista_RDDs++= List(p120_107)
		lista_RDDs++= List(p120_108)
		lista_RDDs++= List(p120_109)
		lista_RDDs++= List(p120_110)
		lista_RDDs++= List(p120_112)
		lista_RDDs++= List(p120_113)
		lista_RDDs++= List(p120_114)
		lista_RDDs++= List(p120_115)
		lista_RDDs++= List(p120_116)
		lista_RDDs++= List(p120_117)
		lista_RDDs++= List(p120_118)
		lista_RDDs++= List(p120_119)
		lista_RDDs++= List(p120_120)
		
		for(a<-0 to 117){
			
			val (_, rasterMetaData) =
      			TileLayerMetadata.fromRdd(lista_RDDs(a), FloatingLayoutScheme(120,140))

			val tiled: RDD[(SpatialKey, MultibandTile)] =
      				lista_RDDs(a)
        			.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        			.repartition(100)
			
			 val reprojected: (RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      				MultibandTileLayerRDD(tiled, rasterMetaData)
		
			lista_RDDs_prontos++=List(reprojected)
		
		}
		
		println(Console.YELLOW+"Carregou direitinho...")
		println(" ")
//--------------------------------------------------------------------------------------------------

		// Combina as tiles no intervalo determinado
		
		println(Console.CYAN+"Realizando a soma dos tiles..."+Console.WHITE+"")
		val p1_total = lista_RDDs_prontos(0)
		println("Realizando a soma dos tiles no intervalo de tempo 2d-1d...")
		val p2_total = lista_RDDs_prontos(1)
		println("Realizando a soma dos tiles no intervalo de tempo 3d-2d...")
		val p3_total = lista_RDDs_prontos(2)
		println("Realizando a soma dos tiles no intervalo de tempo 4d-3d...")
		val p4_total = lista_RDDs_prontos(3)
		println("Realizando a soma dos tiles no intervalo de tempo 5d-4d...")
		val p5_total = lista_RDDs_prontos(4)
		println("Realizando a soma dos tiles no intervalo de tempo 10d-6d...")
		val p10_total = lista_RDDs_prontos(5).++(lista_RDDs_prontos(6))
		                p10_total.++(lista_RDDs_prontos(7)) 
				p10_total.++(lista_RDDs_prontos(8)) 
				p10_total.++(lista_RDDs_prontos(9)) 		
		println("Realizando a soma dos tiles no intervalo de tempo 15d-11d...")
		val p15_total = lista_RDDs_prontos(10).++(lista_RDDs_prontos(11))
		                p15_total.++(lista_RDDs_prontos(12)) 
				p15_total.++(lista_RDDs_prontos(13)) 
				p15_total.++(lista_RDDs_prontos(14))
		println("Realizando a soma dos tiles no intervalo de tempo 30d-16d...")
		val p30_total = lista_RDDs_prontos(15).++(lista_RDDs_prontos(16))
		                p30_total.++(lista_RDDs_prontos(17)) 
				p30_total.++(lista_RDDs_prontos(18)) 
				p30_total.++(lista_RDDs_prontos(19))
				p30_total.++(lista_RDDs_prontos(20)) 
				p30_total.++(lista_RDDs_prontos(21)) 
				p30_total.++(lista_RDDs_prontos(22))
 				p30_total.++(lista_RDDs_prontos(23)) 
				p30_total.++(lista_RDDs_prontos(24)) 
				p30_total.++(lista_RDDs_prontos(25))
				p30_total.++(lista_RDDs_prontos(26)) 
				p30_total.++(lista_RDDs_prontos(27)) 
				p30_total.++(lista_RDDs_prontos(28))
				p30_total.++(lista_RDDs_prontos(29))		
		println("Realizando a soma dos tiles no intervalo de tempo 60d-31d...")
		val p60_total = lista_RDDs_prontos(30).++(lista_RDDs_prontos(31))
		                p60_total.++(lista_RDDs_prontos(32)) 
				p60_total.++(lista_RDDs_prontos(33)) 
				p60_total.++(lista_RDDs_prontos(34))
				p60_total.++(lista_RDDs_prontos(35)) 
				p60_total.++(lista_RDDs_prontos(36)) 
				p60_total.++(lista_RDDs_prontos(37))
 				p60_total.++(lista_RDDs_prontos(38)) 
				p60_total.++(lista_RDDs_prontos(39)) 
				p60_total.++(lista_RDDs_prontos(40))
				p60_total.++(lista_RDDs_prontos(41)) 
				p60_total.++(lista_RDDs_prontos(42)) 
				p60_total.++(lista_RDDs_prontos(43))
				p60_total.++(lista_RDDs_prontos(44))
				p60_total.++(lista_RDDs_prontos(45)) 
				p60_total.++(lista_RDDs_prontos(46)) 
				p60_total.++(lista_RDDs_prontos(47))
				p60_total.++(lista_RDDs_prontos(48)) 
				p60_total.++(lista_RDDs_prontos(49)) 
				p60_total.++(lista_RDDs_prontos(50))
 				p60_total.++(lista_RDDs_prontos(51)) 
				p60_total.++(lista_RDDs_prontos(52)) 
				p60_total.++(lista_RDDs_prontos(53))
				p60_total.++(lista_RDDs_prontos(54)) 
				p60_total.++(lista_RDDs_prontos(55)) 		
		println("Realizando a soma dos tiles no intervalo de tempo 90d-61d...")
		val p90_total = lista_RDDs_prontos(56).++(lista_RDDs_prontos(57))
		                p90_total.++(lista_RDDs_prontos(58)) 
				p90_total.++(lista_RDDs_prontos(59)) 
				p90_total.++(lista_RDDs_prontos(60))
				p90_total.++(lista_RDDs_prontos(61)) 
				p90_total.++(lista_RDDs_prontos(62)) 
				p90_total.++(lista_RDDs_prontos(63))
 				p90_total.++(lista_RDDs_prontos(64)) 
				p90_total.++(lista_RDDs_prontos(65)) 
				p90_total.++(lista_RDDs_prontos(66))
				p90_total.++(lista_RDDs_prontos(67)) 
				p90_total.++(lista_RDDs_prontos(68)) 
				p90_total.++(lista_RDDs_prontos(69))
				p90_total.++(lista_RDDs_prontos(70))
				p90_total.++(lista_RDDs_prontos(71)) 
				p90_total.++(lista_RDDs_prontos(72)) 
				p90_total.++(lista_RDDs_prontos(73))
				p90_total.++(lista_RDDs_prontos(74)) 
				p90_total.++(lista_RDDs_prontos(75)) 
				p90_total.++(lista_RDDs_prontos(76))
 				p90_total.++(lista_RDDs_prontos(77)) 
				p90_total.++(lista_RDDs_prontos(78)) 
				p90_total.++(lista_RDDs_prontos(79))
				p90_total.++(lista_RDDs_prontos(80)) 
				p90_total.++(lista_RDDs_prontos(81)) 
				p90_total.++(lista_RDDs_prontos(82)) 
				p90_total.++(lista_RDDs_prontos(83))
				p90_total.++(lista_RDDs_prontos(84)) 
				p90_total.++(lista_RDDs_prontos(85)) 
				p90_total.++(lista_RDDs_prontos(86))
 				p90_total.++(lista_RDDs_prontos(87)) 
				p90_total.++(lista_RDDs_prontos(88)) 
				p90_total.++(lista_RDDs_prontos(89))
				p90_total.++(lista_RDDs_prontos(90)) 
				p90_total.++(lista_RDDs_prontos(91)) 	
		println("Realizando a soma dos tiles no intervalo de tempo 120d-91d...")
		val p120_total = lista_RDDs_prontos(92).++(lista_RDDs_prontos(93))
		                p120_total.++(lista_RDDs_prontos(94)) 
				p120_total.++(lista_RDDs_prontos(95)) 
				p120_total.++(lista_RDDs_prontos(96))
				p120_total.++(lista_RDDs_prontos(97)) 
				p120_total.++(lista_RDDs_prontos(98)) 
				p120_total.++(lista_RDDs_prontos(99))
 				p120_total.++(lista_RDDs_prontos(100)) 
				p120_total.++(lista_RDDs_prontos(101)) 
				p120_total.++(lista_RDDs_prontos(102))
				p120_total.++(lista_RDDs_prontos(103)) 
				p120_total.++(lista_RDDs_prontos(104)) 
				p120_total.++(lista_RDDs_prontos(105))
				p120_total.++(lista_RDDs_prontos(106))
				p120_total.++(lista_RDDs_prontos(107)) 
				p120_total.++(lista_RDDs_prontos(108)) 
				p120_total.++(lista_RDDs_prontos(109))
				p120_total.++(lista_RDDs_prontos(110)) 
				p120_total.++(lista_RDDs_prontos(111)) 
				p120_total.++(lista_RDDs_prontos(112))
 				p120_total.++(lista_RDDs_prontos(113)) 
				p120_total.++(lista_RDDs_prontos(114)) 
				p120_total.++(lista_RDDs_prontos(115))
				p120_total.++(lista_RDDs_prontos(116)) 
				p120_total.++(lista_RDDs_prontos(117)) 	
		println(Console.YELLOW+"Somou tudo direitinho :) ...")	
		println(" ")
		
		println(Console.CYAN+"Realizando o cálculo do fp..."+Console.WHITE+"")	
		def funcao_1(tile: Tile, const: Double) = {
			tile.mapDouble{x => math.pow(2.718281828459045235360287,x*(const))}
		}

			
		
		println("Realizando o cálculo do fp1...")
		var fp1 = p1_total.map{case(x:Tile) => funcao_1(x,-0.014)}
		println("Realizando o cálculo do fp2...")
		var fp2 = p2_total.map{case(x:Tile) => funcao_1(x,-0.07)}	
		println("Realizando o cálculo do fp3...")
		var fp3 = p3_total.map{case(x:Tile) => funcao_1(x,-0.04)}
		println("Realizando o cálculo do fp4...")
		var fp4 = p4_total.map{case(x:Tile) => funcao_1(x,-0.03)}
		println("Realizando o cálculo do fp5...")
		var fp5 = p5_total.map{case(x:Tile) => funcao_1(x,-0.02)}
		println("Realizando o cálculo do fp10...")
		var fp10 = p10_total.map{case(x:Tile) => funcao_1(x,-0.01)}
		println("Realizando o cálculo do fp15...")
		var fp15 = p15_total.map{case(x:Tile) => funcao_1(x,-0.008)}
		println("Realizando o cálculo do fp30...")
		var fp30 = p30_total.map{case(x:Tile) => funcao_1(x,-0.004)}
		println("Realizando o cálculo do fp60...")
		var fp60 = p60_total.map{case(x:Tile) => funcao_1(x,-0.002)}
		println("Realizando o cálculo do fp90...")
		var fp90 = p90_total.map{case(x:Tile) => funcao_1(x,-0.001)}
		println("Realizando o cálculo do fp120...")
		var fp120 = p120_total.map{case(x:Tile) => funcao_1(x,-0.0007)}
		println(Console.YELLOW+"Realizou os cálculos direitinho :) ...")
		println(" ")
		
		

		

			
		
	}	
}
