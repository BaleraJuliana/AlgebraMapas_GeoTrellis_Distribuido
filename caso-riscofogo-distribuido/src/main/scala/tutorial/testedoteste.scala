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
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._


import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._

import org.apache.spark.rdd._


import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions

import scala.io.StdIn
import java.io.File

object testedoteste {

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
	
//p1

	def run(implicit sc: SparkContext) = {
		println(Console.CYAN+"Lendo precipitação precipitação em formato RDD..."+Console.WHITE+"")
		

		val p1: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07151200${nc}.tif")

		val (_, rasterMetaData_p1) =
      			TileLayerMetadata.fromRdd(p1, FloatingLayoutScheme(120,140))

		val tiled_p1: RDD[(SpatialKey, MultibandTile)] =
      			p1
        			.tileToLayout(rasterMetaData_p1.cellType, rasterMetaData_p1.layout, Bilinear)
        			.repartition(100)
		
      
		
    		
        val reprojected: (RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      		MultibandTileLayerRDD(tiled_p1, rasterMetaData_p1)
        	
    	val attributeStore = FileAttributeStore("data")

   	

    	val p11 = SinglebandGeoTiff(s"data/RiscoFogo/precipitacao_2017/S10648241_201707151200.tif")
    	val cols = p11.tile.cols
    	val rows = p11.tile.rows
	println(Console.CYAN+""+cols+Console.WHITE)
	println(Console.CYAN+""+rows+Console.WHITE)
 	val tile = DoubleArrayTile.empty(cols, rows)
	var linha = 0
	var coluna = 0
	val lista_tile = ListBuffer[(Tile)]()
	
	val lista_posicao_linha = ListBuffer[(Int)]()
	for(i<-1 to 99){
		lista_posicao_linha++=List(140*i)
	}
	val lista_posicao_coluna = ListBuffer[(Int)]()
	for(i<-1 to 99){
		lista_posicao_coluna++=List(120*i)
	}

	var posicao_linha = 0
        var posicao_coluna = 0
	
	




	/**
	println(Console.RED+""+reprojected.collect().size)
    	reprojected
    		.collect().foreach{case(key: SpatialKey, iterator:MultibandTile) =>
      			
			val banda = iterator.band(0)

			if(coluna==1400){
				posicao_coluna = posicao_coluna + 1
			}
			if(linha==1200){
								
				posicao_linha = posicao_linha + 1			
			}

			for(i<-0 to (140)){
				linha = lista_posicao_linha.get(posicao_linha)
				for(j<-0 to (120)){
					println((coluna+i)+","+(linha+j))
				 	var valor_atualizado = banda.get(i,j);			
					tile.setDouble(coluna+i, linha+j, valor_atualizado)				
				}
			}
			posicao_coluna = posicao_coluna + 1
      	}
	SinglebandGeoTiff(tile, p11.extent, p11.crs).write("data/lala.tif")


	**/


	/**
	reprojected
		.foreach{case(key, iterator: Tile) => 
			println(iterator)
		}

	**/

    	//val layerId = LayerId("lala",1)
    	//val writer = FileLayerWriter("data")
    	//val index: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(reprojected.metadata.bounds.get)
    	//writer.write(layerId, reprojected, index)

    	//val mb = ArrayMultibandTile(reprojected).convert(DoubleConstantNoDataCellType)
		//println(Console.CYAN+"Gerando mapa de risco de fogo..."+Console.WHITE+"")
    	//MultibandGeoTiff(mb, p1.extent, p1.crs).write(s"data/saida/mapa_risco_fogo_${ano}.tif")	
    	//val maxWidth = Index.digits(keyIndex.toIndex(keyIndex.keyBounds.maxKey))
    	//val keyPath = KeyPathGenerator("data", path, keyIndex, maxWidth)
      	//FileLayerWriter.write(layerId, reprojected, ZCurveKeyIndexMethod)

      	//val catalogPath = new java.io.File("data").getAbsolutePath
  		// Create a reader that will read in the indexed tiles we produced in IngestImage.
 	 	//val fileValueReader = FileValueReader(catalogPath)
  		//def reader(layerId: LayerId) = fileValueReader.reader[SpatialKey, MultibandTile](layerId)
  		
	}
}
	/**

		
    	reprojected
    		.foreach{case(key,iterator: Tile) =>
      			val tile = DoubleArrayTile.empty(cols, rows)
      			iterator.foreach{value:Tile => 
      					tile.setDouble(0,0,value.get(0,0))
      			}
      			
      		SinglebandGeoTiff(tile, p11.extent, p11.crs).write("data/lala.tif")
    	}

		val p1_geotiff_seq = Seq.apply(p1)
		//paralelizando em 10 partes
		val p1_distribuido = sc.parallelize(p1_geotiff_seq, 10)
		
	
		//p2
		println(Console.CYAN+"Lendo precipitação precipitação 2d-1d..."+Console.WHITE)
		val p2: RDD[(ProjectedExtent, MultibandTile)] =
      			sc.hadoopMultibandGeoTiffRDD(s"data/RiscoFogo/precipitacao_${ano}/S10648241_${ano}07141200${nc}.tif")

		val (_, rasterMetaData_p2) =
      			TileLayerMetadata.fromRdd(p2, FloatingLayoutScheme(512))

		val tiled_p2: RDD[(SpatialKey, MultibandTile)] =
      			p2
        			.tileToLayout(rasterMetaData_p2.cellType, rasterMetaData_p2.layout, Bilinear)
        			.repartition(100)
		//Converter para Seq (pq o parallelize só le Seq)
		//val p2_geotiff_seq = Seq.apply(p2)
		//paralelizando em 10 partes
		//val p2_distribuido = sc.parallelize(p2_geotiff_seq, 10)
		
		
		//println("lalala: "+raster)
		
		//def funcao(tile: Tile) = {
		//	tile.map(x => x+1)
		//}
		println(Console.CYAN+"Realizando o cálculo do fp..."+Console.WHITE+"")	
		def funcao_1(tile: Tile, const: Double) = {
			tile.mapDouble{x => math.pow(2.718281828459045235360287,x*(const))}
		}
					
		println("chamando ")
		//val e_isso_mesmo_producao = p1.map{case(e: ProjectedExtent, t: MultibandTile) => funcao(t)}
		var fp1 = p1.map{case(extent: ProjectedExtent, x:Tile) => funcao_1(x,-0.014)}
		var fp2 = p2.map{case(extent: ProjectedExtent, x:Tile) => funcao_1(x,-0.07)}
		
		fp1.zip(fp2).map{case(esquerda, direita) => esquerda + direita}

		// a partir daqui eu to tentando multiplicar dois tiles (mas como fazer isso ??????? )
		//val a = fp1.map{case(x:Tile) => funcao_2(x,fp2)}
		
		def funcao_2(tile: Tile, rdd: RDD[(ProjectedExtent, MultibandTile)]) = {
			tile.mapDouble{x => x*2}
		}

		val a = fp1.map{case(x:Tile) => funcao_2(x,p2)}
		
		//fp1.union(fp2)
	
		def multiplicar_tiles(tile1: Tile, tile2: Tile): Tile = {
			return tile1*tile2
		}


		
		println("fim!")


		
		//mais_um.map(line => println(line))
		
		println(" ")
		println(" ")
		println(" ")
		println("iiii ")
		val input_a = Array(1.0,1.0,1.0,1.0,1.0)
		val input_b = Array(2.0,2.0,2.0,2.0,2.0)
		
		val lala = sc.parallelize(input_a,2)
		val lele = sc.parallelize(input_b,2)
		
		//rdd.foreach(x => 2)
		//input_a.map(line => println(line))
		//input_b.map(line => println(line))
		
		//val bebe = input_a.zip(input_b)
		//bebe.map(line => println(line))
		def dotprod(x: Array[Double], y: Array[Double]) = x.zip(y).map(x => x._1 * x._2).reduce(_+_)
		val lolo = dotprod(input_a, input_b)
		println(lolo)
		val lili = input_a.zip(input_b).map{case(direita, esquerda) => direita+esquerda}


		val mais_um = input_a.foreach{x => 
				math.pow(x,-15);
				println(math.pow(x,-0.5))}

		//val lili = input_b.union(input_a,)
		lili.map(line => println(line))
		//println("lalala"+lili.partitions.length)
		

		
		println(Console.CYAN+"Realizando o cálculo do fp..."+Console.WHITE+"")		
		println("Realizando o cálculo do fp1...")
		var fp1 = p1_total.map{rdd => println(rdd.id)}

		println(Console.CYAN+"Realizando o cálculo do fp..."+Console.WHITE+"")		
		println("Realizando o cálculo do fp1...")
		var fp1 = p1_total.tile.localPow(-0.14)
		fp1 = fp1.tile.localPow(2.718281828459045235360287)		
		println("Realizando o cálculo do fp2...")
		var fp2 = p2_total.tile.localPow(-0.07)	
		fp2 = fp2.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp3...")
		var fp3 = p3_total.tile.localPow(-0.04)
		fp3 = fp3.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp4...")
		var fp4 = p4_total.tile.localPow(-0.03)
		fp4 = fp4.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp5...")
		var fp5 = p5_total.tile.localPow(-0.02)
		fp5 = fp5.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp10...")
		var fp10 = p10_total.tile.localPow(-0.01)
		fp10 = fp10.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp15...")
		var fp15 = p15_total.tile.localPow(-0.008)
		fp15 = fp15.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp30...")
		var fp30 = p30_total.tile.localPow(-0.004)
		fp30 = fp30.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp60...")
		var fp60 = p60_total.tile.localPow(-0.002)
		fp60 = fp60.tile.localPow(2.718281828459045235360287)			
		println("Realizando o cálculo do fp90...")
		var fp90 = p90_total.tile.localPow(-0.001)
		fp90 = fp90.tile.localPow(2.718281828459045235360287)	
		println("Realizando o cálculo do fp120...")
		var fp120 = p120_total.tile.localPow(-0.0007)
		fp120 = fp120.tile.localPow(2.718281828459045235360287)			
		println(Console.YELLOW+"Realizou os cálculos direitinho :) ...")
		println(" ")
		
		**/		
